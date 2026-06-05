-- The generic (variable-length) base58 encoder and decoder use a big-integer base conversion that is
-- quadratic in the input length. Previously they ran to completion without checking for cancellation,
-- so a single large value could keep a thread busy for many minutes, ignoring `max_execution_time`.
-- The server-side AST fuzzer (serverfuzz stress test) repeatedly hit this, blocking the connection
-- handler and triggering "Hung check failed, possible deadlock found".
--
-- This is fixed in two complementary ways: base58 inputs are limited to 10 KB by default (base58 is meant
-- for short data such as keys, hashes and addresses; the limit is configurable via the setting
-- `function_base58_max_input_size`), and the conversion now checks for cancellation periodically.

-- 1. Oversized inputs are rejected outright (the default limit is 10 KB).
SELECT base58Encode(randomString(10001)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT base58Decode(repeat('z', 10001)); -- { serverError TOO_LARGE_STRING_SIZE }

-- 2. tryBase58Decode keeps its "empty string on error" contract for oversized input.
SELECT tryBase58Decode(repeat('z', 10001)) = '';

-- 3. Inputs within the limit still round-trip (the encoded form is also within the limit).
SELECT base58Decode(base58Encode(repeat('a', 5000))) = repeat('a', 5000);

-- 4. The limit is configurable: a smaller limit rejects smaller inputs.
SELECT base58Encode(randomString(100)) SETTINGS function_base58_max_input_size = 10; -- { serverError TOO_LARGE_STRING_SIZE }

-- 5. The limit can be disabled (the pre-26.6 behavior, also what `compatibility` restores). 10001 bytes is far
--    below the size at which the quadratic cost matters, so this stays fast even with the limit disabled.
SELECT length(base58Encode(randomString(10001))) > 10001 SETTINGS function_base58_max_input_size = 0;

-- 6. The linear base32/base64 functions are never limited and ignore the setting.
SELECT base64Decode(base64Encode(s)) = s FROM (SELECT randomString(100000) AS s) SETTINGS function_base58_max_input_size = 10;

-- 7. Many in-limit values in a single block must still respect the time limit: cancellation is checked
--    inside the conversion, not only between pipeline blocks.
SELECT base58Encode(randomString(10000)) FROM numbers(1000) FORMAT Null SETTINGS max_execution_time = 1; -- { serverError TIMEOUT_EXCEEDED }

SELECT 'ok';
