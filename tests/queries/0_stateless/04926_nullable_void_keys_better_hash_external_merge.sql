-- The set (void-mapped) counterpart of 04836. `GROUP BY` without aggregate functions on a nullable
-- fixed-width key uses a set method, and the external-aggregation merge re-aggregates the spilled stream
-- with a method hashing over more than 32 bits - `nullable_key64_void_hash64` /
-- `nullable_keys128_void_hash64` / `nullable_keys256_void_hash64`. Those are new instantiations, so check
-- that each still groups exactly as the in-memory aggregation does, the NULL group included: for a single
-- nullable key it lives in a slot beside the cells, for the packed keys it is folded into the key.
-- Two `UInt32` keys fit the 128-bit packed form and two `UInt64` keys need the 256-bit one.

SET max_bytes_before_external_group_by = 1;
SET max_threads = 4;
SET group_by_two_level_threshold = 100;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'nullable_key64_void';
SELECT count(), countIf(k IS NULL), sum(k), min(k), max(k)
FROM (SELECT nullIf(toUInt64(number % 5000), 3) AS k FROM numbers_mt(200000) GROUP BY k);

SELECT 'nullable_keys128_void';
SELECT count(), countIf(a IS NULL), countIf(b IS NULL), sum(a), sum(b)
FROM (SELECT nullIf(toUInt32(number % 2000), 7) AS a, nullIf(toUInt32(number % 11), 5) AS b
      FROM numbers_mt(200000) GROUP BY a, b);

SELECT 'nullable_keys256_void';
SELECT count(), countIf(a IS NULL), countIf(b IS NULL), sum(a), sum(b)
FROM (SELECT nullIf(toUInt64(number % 2000), 7) AS a, nullIf(toUInt64(number % 11), 5) AS b
      FROM numbers_mt(200000) GROUP BY a, b);

SELECT 'the spilled result matches the in-memory one';
SELECT
    (SELECT sum(cityHash64(k)) FROM (SELECT nullIf(toUInt64(number % 5000), 3) AS k FROM numbers_mt(200000) GROUP BY k))
  = (SELECT sum(cityHash64(k)) FROM (SELECT nullIf(toUInt64(number % 5000), 3) AS k FROM numbers_mt(200000) GROUP BY k) SETTINGS max_bytes_before_external_group_by = 0),
    (SELECT sum(cityHash64(a, b)) FROM (SELECT nullIf(toUInt32(number % 2000), 7) AS a, nullIf(toUInt32(number % 11), 5) AS b FROM numbers_mt(200000) GROUP BY a, b))
  = (SELECT sum(cityHash64(a, b)) FROM (SELECT nullIf(toUInt32(number % 2000), 7) AS a, nullIf(toUInt32(number % 11), 5) AS b FROM numbers_mt(200000) GROUP BY a, b) SETTINGS max_bytes_before_external_group_by = 0),
    (SELECT sum(cityHash64(a, b)) FROM (SELECT nullIf(toUInt64(number % 2000), 7) AS a, nullIf(toUInt64(number % 11), 5) AS b FROM numbers_mt(200000) GROUP BY a, b))
  = (SELECT sum(cityHash64(a, b)) FROM (SELECT nullIf(toUInt64(number % 2000), 7) AS a, nullIf(toUInt64(number % 11), 5) AS b FROM numbers_mt(200000) GROUP BY a, b) SETTINGS max_bytes_before_external_group_by = 0);
