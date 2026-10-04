-- A millisecond setting whose value does not fit Int64 microseconds is rejected
-- instead of silently wrapping (2^61 used to become 0, UInt64 max used to become -1 ms).
SELECT 1 SETTINGS connection_pool_max_wait_ms = 2305843009213693952; -- { clientError BAD_ARGUMENTS }
SELECT 1 SETTINGS connection_pool_max_wait_ms = 18446744073709551615; -- { clientError BAD_ARGUMENTS }
SELECT 1 SETTINGS connection_pool_max_wait_ms = 9223372036854776; -- { clientError BAD_ARGUMENTS }

-- The largest value that fits is accepted and stored exactly.
SELECT value FROM system.settings WHERE name = 'connection_pool_max_wait_ms' SETTINGS connection_pool_max_wait_ms = 9223372036854775;
