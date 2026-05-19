-- packed_skip_index_max_bytes takes effect at write time: existing parts keep whatever layout
-- they had when they were written, and future INSERTs / merges apply the current setting.
-- Verify both directions (off->on, on->off) preserve readability of the old part and adopt
-- the new layout for new parts.
--
-- For every scenario we check three things:
--   1. secondary_indices_compressed_bytes > 0  -- the index is materialized,
--   2. count() matches expectations           -- the data is queryable,
--   3. EXPLAIN indexes = 1 shows kept < total -- the index actually GATES GRANULES under the
--                                              new layout. Random merge-tree settings perturb
--                                              the absolute kept/total numbers, so we only
--                                              assert "kept < total" qualitatively.
--
-- Test both Compact and Wide part layouts.

-- ------------------------------------------------------------------
-- Wide: threshold raised from 0 (packing disabled) to 1 MiB.
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_wide_off_to_on;
CREATE TABLE t_wide_off_to_on
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_wide_off_to_on SELECT number, number * 7 FROM numbers(10000);
SELECT 'wide_off2on_index_materialized_before', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_off_to_on' AND active;

ALTER TABLE t_wide_off_to_on MODIFY SETTING packed_skip_index_max_bytes = '1M';

-- Old part still queries correctly with the new setting in metadata.
SELECT 'wide_off2on_query_old_part', count() FROM t_wide_off_to_on WHERE v BETWEEN 70 AND 700;

-- New INSERT picks up the new setting; both parts must still materialize the index.
INSERT INTO t_wide_off_to_on SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'wide_off2on_index_materialized_after_insert', name, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_off_to_on' AND active
ORDER BY name;
SELECT 'wide_off2on_combined_count', count() FROM t_wide_off_to_on WHERE v BETWEEN 70 AND 700;

-- Merge: the resulting single part uses the current writer setting; the index is still there.
OPTIMIZE TABLE t_wide_off_to_on FINAL;
SELECT 'wide_off2on_index_materialized_after_merge', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_off_to_on' AND active;
SELECT 'wide_off2on_count_after_merge', count() FROM t_wide_off_to_on WHERE v BETWEEN 70 AND 700;
SELECT 'wide_off2on_index_filters_after_merge', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_wide_off_to_on WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_wide_off_to_on SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wide_off_to_on;

-- ------------------------------------------------------------------
-- Wide: threshold lowered from 1 MiB to 0 (packing disabled).
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_wide_on_to_off;
CREATE TABLE t_wide_on_to_off
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_wide_on_to_off SELECT number, number * 7 FROM numbers(10000);
SELECT 'wide_on2off_index_materialized_before', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_on_to_off' AND active;

ALTER TABLE t_wide_on_to_off MODIFY SETTING packed_skip_index_max_bytes = 0;

SELECT 'wide_on2off_query_old_part', count() FROM t_wide_on_to_off WHERE v BETWEEN 70 AND 700;

INSERT INTO t_wide_on_to_off SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'wide_on2off_index_materialized_after_insert', name, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_on_to_off' AND active
ORDER BY name;
SELECT 'wide_on2off_combined_count', count() FROM t_wide_on_to_off WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_wide_on_to_off FINAL;
SELECT 'wide_on2off_index_materialized_after_merge', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_on_to_off' AND active;
SELECT 'wide_on2off_count_after_merge', count() FROM t_wide_on_to_off WHERE v BETWEEN 70 AND 700;
SELECT 'wide_on2off_index_filters_after_merge', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_wide_on_to_off WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_wide_on_to_off SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wide_on_to_off;

-- ------------------------------------------------------------------
-- Compact: threshold raised from 0 (packing disabled) to 1 MiB.
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_compact_off_to_on;
CREATE TABLE t_compact_off_to_on
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS packed_skip_index_max_bytes = 0, auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_compact_off_to_on SELECT number, number * 7 FROM numbers(10000);
SELECT 'compact_off2on_index_materialized_before', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_off_to_on' AND active;

ALTER TABLE t_compact_off_to_on MODIFY SETTING packed_skip_index_max_bytes = '1M';

SELECT 'compact_off2on_query_old_part', count() FROM t_compact_off_to_on WHERE v BETWEEN 70 AND 700;

INSERT INTO t_compact_off_to_on SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'compact_off2on_index_materialized_after_insert', name, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_off_to_on' AND active
ORDER BY name;
SELECT 'compact_off2on_combined_count', count() FROM t_compact_off_to_on WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_compact_off_to_on FINAL;
SELECT 'compact_off2on_index_materialized_after_merge', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_off_to_on' AND active;
SELECT 'compact_off2on_count_after_merge', count() FROM t_compact_off_to_on WHERE v BETWEEN 70 AND 700;
SELECT 'compact_off2on_index_filters_after_merge', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_compact_off_to_on WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_compact_off_to_on SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_off_to_on;

-- ------------------------------------------------------------------
-- Compact: threshold lowered from 1 MiB to 0 (packing disabled).
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_compact_on_to_off;
CREATE TABLE t_compact_on_to_off
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS packed_skip_index_max_bytes = '1M', auto_statistics_types = '', index_granularity = 1024;

INSERT INTO t_compact_on_to_off SELECT number, number * 7 FROM numbers(10000);
SELECT 'compact_on2off_index_materialized_before', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_on_to_off' AND active;

ALTER TABLE t_compact_on_to_off MODIFY SETTING packed_skip_index_max_bytes = 0;

SELECT 'compact_on2off_query_old_part', count() FROM t_compact_on_to_off WHERE v BETWEEN 70 AND 700;

INSERT INTO t_compact_on_to_off SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'compact_on2off_index_materialized_after_insert', name, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_on_to_off' AND active
ORDER BY name;
SELECT 'compact_on2off_combined_count', count() FROM t_compact_on_to_off WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_compact_on_to_off FINAL;
SELECT 'compact_on2off_index_materialized_after_merge', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_on_to_off' AND active;
SELECT 'compact_on2off_count_after_merge', count() FROM t_compact_on_to_off WHERE v BETWEEN 70 AND 700;
SELECT 'compact_on2off_index_filters_after_merge', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_compact_on_to_off WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_compact_on_to_off SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_on_to_off;

-- ------------------------------------------------------------------
-- Regression: setting flip + ALTER UPDATE on indexed column.
-- The transition writer ignores the source layout and uses the new setting:
-- per-file -> packed must drop the stale per-substream checksum entries,
-- packed -> per-file must drop the stale archive checksum entry. CHECK TABLE
-- catches either if they linger.
-- ------------------------------------------------------------------
DROP TABLE IF EXISTS t_wide_flip_mutate_off2on;
CREATE TABLE t_wide_flip_mutate_off2on
(
    id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, auto_statistics_types = '', index_granularity = 1024;
INSERT INTO t_wide_flip_mutate_off2on SELECT number, number * 7 FROM numbers(10000);
ALTER TABLE t_wide_flip_mutate_off2on MODIFY SETTING packed_skip_index_max_bytes = '1M';
ALTER TABLE t_wide_flip_mutate_off2on UPDATE v = v + 1 WHERE id < 100 SETTINGS mutations_sync = 2;
SELECT 'wide_flip_off2on_count', count() FROM t_wide_flip_mutate_off2on WHERE v BETWEEN 70 AND 700;
SELECT 'wide_flip_off2on_index_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_wide_flip_mutate_off2on WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_wide_flip_mutate_off2on SETTINGS check_query_single_value_result = 1;
DROP TABLE t_wide_flip_mutate_off2on;

DROP TABLE IF EXISTS t_wide_flip_mutate_on2off;
CREATE TABLE t_wide_flip_mutate_on2off
(
    id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', auto_statistics_types = '', index_granularity = 1024;
INSERT INTO t_wide_flip_mutate_on2off SELECT number, number * 7 FROM numbers(10000);
ALTER TABLE t_wide_flip_mutate_on2off MODIFY SETTING packed_skip_index_max_bytes = 0;
ALTER TABLE t_wide_flip_mutate_on2off UPDATE v = v + 1 WHERE id < 100 SETTINGS mutations_sync = 2;
SELECT 'wide_flip_on2off_count', count() FROM t_wide_flip_mutate_on2off WHERE v BETWEEN 70 AND 700;
SELECT 'wide_flip_on2off_index_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_wide_flip_mutate_on2off WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_wide_flip_mutate_on2off SETTINGS check_query_single_value_result = 1;
DROP TABLE t_wide_flip_mutate_on2off;
