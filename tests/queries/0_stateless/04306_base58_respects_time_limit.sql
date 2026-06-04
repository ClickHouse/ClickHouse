-- The generic (variable-length) base58 encoder and decoder use a big-integer base conversion that is
-- quadratic in the input length. Previously they ran to completion without checking for cancellation,
-- so a single large value could keep a thread busy for many minutes, ignoring `max_execution_time`.
-- The server-side AST fuzzer (serverfuzz stress test) repeatedly hit this, blocking the connection
-- handler and triggering "Hung check failed, possible deadlock found".
--
-- These queries must terminate quickly with TIMEOUT_EXCEEDED instead of hanging.

SET max_execution_time = 3;

SELECT length(base58Encode(randomString(20000000))) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }
SELECT length(base58Decode(repeat('z', 1000000))) FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SELECT 'ok';
