-- max_bytes_in_distinct / max_bytes_in_set must count string keys, which live in the
-- string_pool arena, not in the hash table buffer. Before the fix the limits saw only the
-- ~24-bytes-per-key buffer, so DISTINCT/IN over string keys could hold memory exceeding the
-- limit by the whole key payload (unbounded in the key length).

SET max_threads = 1;
SET max_block_size = 8192;

-- 10000 distinct ~1 KB keys = ~10 MB of state vs a 1 MB limit. The hash buffer alone
-- (2^14 cells) stays well under the limit, so without arena accounting nothing trips.

SELECT '-- DISTINCT, throw: trips on string payload, not only on the hash buffer';
SELECT count() FROM (SELECT DISTINCT toString(number) || repeat('x', 1000) AS s FROM numbers(10000))
SETTINGS max_bytes_in_distinct = 1000000, distinct_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- DISTINCT, break: stops near the limit instead of retaining the full payload';
-- small blocks so the limit does not trip on the very first chunk and the count stays low
-- whether or not the chunk that crosses the limit is emitted (see #110075)
SELECT count() > 0, count() < 5000 FROM (SELECT DISTINCT toString(number) || repeat('x', 1000) AS s FROM numbers(10000))
SETTINGS max_bytes_in_distinct = 1000000, distinct_overflow_mode = 'break', max_block_size = 256;

SELECT '-- IN set, throw: trips on string payload';
SELECT count() FROM numbers(10) WHERE toString(number) IN (SELECT toString(number) || repeat('x', 1000) FROM numbers(10000))
SETTINGS max_bytes_in_set = 1000000, set_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- fixed-size keys are unaffected: the same limit passes without string keys';
SELECT count() FROM (SELECT DISTINCT number FROM numbers(10000))
SETTINGS max_bytes_in_distinct = 1000000, distinct_overflow_mode = 'throw';
