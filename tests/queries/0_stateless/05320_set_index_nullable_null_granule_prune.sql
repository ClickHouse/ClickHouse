-- A `set` skip index on a `Nullable` column skips a granule whose set holds NULL but not the searched value.
-- https://github.com/ClickHouse/ClickHouse/issues/123705

DROP TABLE IF EXISTS t_set_nullable_prune;

CREATE TABLE t_set_nullable_prune (k UInt32, c Nullable(UInt32), INDEX idx_c_set c TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';

-- Granules: {NULL, 1, 9, 9}, {NULL, 5, 1, 9}, {1, 9, 1, 9}, {NULL, NULL, 1, 9}. Each covers the range [1, 9],
-- so only the set index can skip one.
INSERT INTO t_set_nullable_prune VALUES
    (0, NULL), (1, 1), (2, 9), (3, 9), (4, NULL), (5, 5), (6, 1), (7, 9),
    (8, 1), (9, 9), (10, 1), (11, 9), (12, NULL), (13, NULL), (14, 1), (15, 9);
OPTIMIZE TABLE t_set_nullable_prune FINAL;

SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0,
    transform_null_in = 0, enable_parallel_replicas = 0;

-- Granules the set index keeps (the last `Granules:` line is the skip index).
SELECT 'in, bulk', arrayElement(groupArray(toUInt32(extract(explain, 'Granules: ([0-9]+)/'))), -1)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c IN (5)
      SETTINGS secondary_indices_enable_bulk_filtering = 1)
WHERE explain LIKE '%Granules: %/%';
SELECT 'in, no bulk', arrayElement(groupArray(toUInt32(extract(explain, 'Granules: ([0-9]+)/'))), -1)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c IN (5)
      SETTINGS secondary_indices_enable_bulk_filtering = 0)
WHERE explain LIKE '%Granules: %/%';
SELECT 'equals, bulk', arrayElement(groupArray(toUInt32(extract(explain, 'Granules: ([0-9]+)/'))), -1)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c = 5
      SETTINGS secondary_indices_enable_bulk_filtering = 1)
WHERE explain LIKE '%Granules: %/%';
SELECT 'equals, no bulk', arrayElement(groupArray(toUInt32(extract(explain, 'Granules: ([0-9]+)/'))), -1)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c = 5
      SETTINGS secondary_indices_enable_bulk_filtering = 0)
WHERE explain LIKE '%Granules: %/%';
SELECT 'absent, bulk', arrayElement(groupArray(toUInt32(extract(explain, 'Granules: ([0-9]+)/'))), -1)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c IN (2)
      SETTINGS secondary_indices_enable_bulk_filtering = 1)
WHERE explain LIKE '%Granules: %/%';
SELECT 'absent, no bulk', arrayElement(groupArray(toUInt32(extract(explain, 'Granules: ([0-9]+)/'))), -1)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c IN (2)
      SETTINGS secondary_indices_enable_bulk_filtering = 0)
WHERE explain LIKE '%Granules: %/%';
SELECT 'index used', countIf(explain LIKE '%Name: idx_c_set%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_nullable_prune WHERE c IN (5));

-- The result must not depend on the index: each predicate with and without it.
SELECT 'in', count() FROM t_set_nullable_prune WHERE c IN (5);
SELECT 'in', count() FROM t_set_nullable_prune WHERE c IN (5) SETTINGS use_skip_indexes = 0;
SELECT 'ifNull', count() FROM t_set_nullable_prune WHERE ifNull(c = 5, 1);
SELECT 'ifNull', count() FROM t_set_nullable_prune WHERE ifNull(c = 5, 1) SETTINGS use_skip_indexes = 0;
SELECT 'is null', count() FROM t_set_nullable_prune WHERE (c = 5) IS NULL;
SELECT 'is null', count() FROM t_set_nullable_prune WHERE (c = 5) IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'or is null', count() FROM t_set_nullable_prune WHERE c = 5 OR c IS NULL;
SELECT 'or is null', count() FROM t_set_nullable_prune WHERE c = 5 OR c IS NULL SETTINGS use_skip_indexes = 0;
SELECT 'not in', count() FROM t_set_nullable_prune WHERE NOT (c IN (5));
SELECT 'not in', count() FROM t_set_nullable_prune WHERE NOT (c IN (5)) SETTINGS use_skip_indexes = 0;
SELECT 'not uint8', count() FROM t_set_nullable_prune WHERE NOT toUInt8(c = 5);
SELECT 'not uint8', count() FROM t_set_nullable_prune WHERE NOT toUInt8(c = 5) SETTINGS use_skip_indexes = 0;

DROP TABLE t_set_nullable_prune;
