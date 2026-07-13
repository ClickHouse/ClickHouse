-- max_bytes_in_distinct and max_bytes_in_set must account for the bytes of string keys.
-- With the `key_string` / `key_fixed_string` set methods the hash table holds only fixed-width
-- `StringRef` entries; the key bytes themselves live in the set's `string_pool` arena. The byte
-- limits are checked against the pool plus the hash table buffer, so with ~1 KB keys a 100 KB
-- limit must trip after roughly a hundred keys - far before the 1000 distinct input values end.
-- Multi-column string keys go through the `hashed` method (a 128-bit hash per key, the values
-- themselves are not stored), so the same limit must NOT trip there.

SET max_threads = 1;
SET max_block_size = 10;

SELECT '-- DISTINCT over String keys, break: partial result bounded by the byte limit';
SELECT count() > 0, count() < 500 FROM (SELECT DISTINCT s FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s FROM numbers(1000)))
SETTINGS distinct_overflow_mode = 'break', max_bytes_in_distinct = 100000;

SELECT '-- DISTINCT over String keys, throw';
SELECT count() FROM (SELECT DISTINCT s FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s FROM numbers(1000)))
SETTINGS distinct_overflow_mode = 'throw', max_bytes_in_distinct = 100000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- DISTINCT over FixedString keys, throw: key_fixed_string also stores keys in the pool';
SELECT count() FROM (SELECT DISTINCT s FROM (SELECT toFixedString(concat(toString(number), repeat('x', 990)), 1000) AS s FROM numbers(1000)))
SETTINGS distinct_overflow_mode = 'throw', max_bytes_in_distinct = 100000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- IN set built from String keys, throw (default set_overflow_mode)';
SELECT count() FROM numbers(10) WHERE concat(toString(number), repeat('x', 1000)) IN (SELECT concat(toString(number), repeat('x', 1000)) FROM numbers(1000))
SETTINGS max_bytes_in_set = 100000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- IN set built from String keys, break: the probed keys are inserted before the limit trips';
SELECT count() FROM numbers(10) WHERE concat(toString(number), repeat('x', 1000)) IN (SELECT concat(toString(number), repeat('x', 1000)) FROM numbers(1000))
SETTINGS max_bytes_in_set = 100000, set_overflow_mode = 'break';

SELECT '-- multi-column string keys use the hashed method (values not stored): the same limit must not trip';
SELECT count() FROM (SELECT DISTINCT s1, s2 FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s1, concat(toString(number), repeat('y', 1000)) AS s2 FROM numbers(100)))
SETTINGS distinct_overflow_mode = 'throw', max_bytes_in_distinct = 100000;

SELECT '-- numeric keys are unaffected: a limit far above the fixed allocations does not trip';
SELECT count() FROM (SELECT DISTINCT number FROM numbers(1000))
SETTINGS distinct_overflow_mode = 'throw', max_bytes_in_distinct = 10000000;
