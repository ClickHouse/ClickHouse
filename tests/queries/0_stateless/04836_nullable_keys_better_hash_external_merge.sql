-- External aggregation merges partitions whose combined key count can far exceed 4 billion, so
-- `mergeBlocks` re-aggregates the spilled stream with a method that hashes over more than 32 bits. The
-- nullable fixed-width keys take that path too, which re-aggregates them with `nullable_key64_hash64` /
-- `nullable_keys128_hash64` / `nullable_keys256_hash64`. Those are new instantiations, so check that each
-- still groups exactly as the in-memory aggregation does - including the NULL group, which for a single
-- nullable key lives in a slot outside the cells and for the packed keys is folded into the key itself.
-- The key widths below are what selects each method: a nullable key is packed together with its null map,
-- so two `UInt32` keys fit in 128 bits while two `UInt64` keys need 256.

SET max_bytes_before_external_group_by = 1;
SET max_threads = 4;
SET group_by_two_level_threshold = 100;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'nullable_key64';
SELECT count(), countIf(k IS NULL), sum(k), min(k), max(k)
FROM (SELECT nullIf(toUInt64(number % 5000), 3) AS k FROM numbers_mt(200000) GROUP BY k);

SELECT 'nullable_keys128';
SELECT count(), countIf(a IS NULL), countIf(b IS NULL), sum(a), sum(b)
FROM (SELECT nullIf(toUInt32(number % 2000), 7) AS a, nullIf(toUInt32(number % 11), 5) AS b
      FROM numbers_mt(200000) GROUP BY a, b);

SELECT 'nullable_keys256';
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
