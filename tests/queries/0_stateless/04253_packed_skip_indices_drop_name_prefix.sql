-- Regression: DROP INDEX must not collide with another index whose getIndexFileName begins with
-- the dropped one's filename. With escape_index_filenames=0 the on-disk substream filenames for
-- indices `a` and `a.b` share the `skp_idx_a` prefix; the archive filter must match exact
-- substream filenames, not "<prefix>." starts_with patterns, otherwise dropping `a` would also
-- erase `a.b`'s data inside skp_idx.packed.

DROP TABLE IF EXISTS t_drop_prefix_a;
CREATE TABLE t_drop_prefix_a
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX `a` v TYPE minmax GRANULARITY 1,
    INDEX `a.b` w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS escape_index_filenames = 0,
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_drop_prefix_a SELECT number, number * 2, number * 3 FROM numbers(2000);

ALTER TABLE t_drop_prefix_a DROP INDEX `a` SETTINGS mutations_sync = 2;

SELECT 'drop_a_query_ab', count() FROM t_drop_prefix_a WHERE w BETWEEN 100 AND 200;
-- Prove the surviving index still gates granules. With the prefix bug, the index data was
-- gone from the archive and the planner would fall back to scanning all granules. Random
-- merge-tree settings make the absolute granule count vary, so we only assert that the Skip
-- block's "kept" count is strictly less than the "total" count.
SELECT 'drop_a_ab_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_drop_prefix_a WHERE w = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_drop_prefix_a SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_prefix_a;

-- Symmetric scenario: dropping the longer-named index must not affect the shorter one.
DROP TABLE IF EXISTS t_drop_prefix_ab;
CREATE TABLE t_drop_prefix_ab
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX `a` v TYPE minmax GRANULARITY 1,
    INDEX `a.b` w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS escape_index_filenames = 0,
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_drop_prefix_ab SELECT number, number * 2, number * 3 FROM numbers(2000);

ALTER TABLE t_drop_prefix_ab DROP INDEX `a.b` SETTINGS mutations_sync = 2;

SELECT 'drop_ab_query_a', count() FROM t_drop_prefix_ab WHERE v BETWEEN 100 AND 200;
SELECT 'drop_ab_a_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_drop_prefix_ab WHERE v = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_drop_prefix_ab SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_prefix_ab;
