/**
 * Idempotency middleware for POST requests.
 *
 * If the client sends header `Idempotency-Key`, we will:
 * - return the previously stored response for the same (agent_id, key) when it exists
 * - otherwise allow the request to proceed and store the response on success.
 *
 * This prevents accidental duplicates when clients retry after timeouts / ambiguous failures.
 */

const crypto = require('crypto');
const { queryOne, query } = require('../config/database');

function stableStringify(obj) {
  // good-enough for our use: avoids key-order differences
  if (obj === null || obj === undefined) return '';
  if (typeof obj === 'string') return obj;
  return JSON.stringify(obj, Object.keys(obj).sort());
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.methods] - methods to enforce (default: ['POST'])
 */
function idempotency(opts = {}) {
  const methods = (opts.methods || ['POST']).map((m) => m.toUpperCase());

  return async function idempotencyMiddleware(req, res, next) {
    try {
      if (!methods.includes(String(req.method).toUpperCase())) return next();
      if (!req.agent || !req.agent.id) return next();

      const key = String(req.get('Idempotency-Key') || '').trim();
      if (!key) return next();

      // Keep keys reasonably small (protect DB)
      if (key.length > 200) return next();

      const route = `${req.baseUrl || ''}${req.path || ''}`;
      const requestHash = sha256(
        `${req.method}:${route}:${stableStringify(req.body)}`
      );

      const existing = await queryOne(
        `SELECT status_code, response_body, request_hash
         FROM idempotency_keys
         WHERE agent_id = $1 AND idem_key = $2 AND method = $3 AND route = $4`,
        [req.agent.id, key, req.method, route]
      );

      if (existing) {
        // If the key was re-used with a different payload, treat as a client bug.
        if (existing.request_hash && existing.request_hash !== requestHash) {
          return res.status(409).json({
            success: false,
            error: 'Idempotency-Key reuse with different request payload'
          });
        }

        res.set('Idempotent-Replay', 'true');
        const status = existing.status_code || 200;
        const body = existing.response_body || { success: true };
        return res.status(status).json(body);
      }

      // Capture response so we can store it.
      const originalJson = res.json.bind(res);
      res.json = (body) => {
        res.locals.__idemBody = body;
        return originalJson(body);
      };

      const originalSend = res.send.bind(res);
      res.send = (body) => {
        // If something uses send(string), store as string.
        res.locals.__idemBody = body;
        return originalSend(body);
      };

      res.on('finish', async () => {
        try {
          // Store only successful responses (2xx). Avoid storing errors.
          if (res.statusCode < 200 || res.statusCode >= 300) return;

          const responseBody = res.locals.__idemBody;
          // If body isn't JSON-serializable, skip.
          const responseBodyJson =
            typeof responseBody === 'string'
              ? responseBody
              : JSON.stringify(responseBody);

          await query(
            `INSERT INTO idempotency_keys
               (agent_id, idem_key, method, route, request_hash, status_code, response_body)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (agent_id, idem_key, method, route)
             DO NOTHING`,
            [
              req.agent.id,
              key,
              req.method,
              route,
              requestHash,
              res.statusCode,
              responseBodyJson
            ]
          );
        } catch {
          // best-effort; do not crash request lifecycle
        }
      });

      return next();
    } catch (err) {
      return next(err);
    }
  };
}

module.exports = { idempotency };
