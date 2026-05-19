-- Round-trip test for the packed_skip_index_max_bytes setting: a minmax index written into
-- skp_idx.packed must be queryable AND actually filter granules across Compact / Wide part
-- layouts and across merges that rewrite the archive. State is checked via system tables and
-- EXPLAIN indexes = 1 only (no on-disk file listing) so the test works on any storage.

-- ---------------------------------------------------------------------------------------------
-- Wide part with packed minmax.
-- ---------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS t_packed_wide;
CREATE TABLE t_packed_wide
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4194304, index_granularity = 1024;

INSERT INTO t_packed_wide SELECT number, number * 7 FROM numbers(10000);

SELECT 'wide_count', count() FROM t_packed_wide WHERE v BETWEEN 70 AND 700;
SELECT 'wide_secondary_bytes_positive', secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_wide' AND active;
-- Index must actually drop granules for an in-range predicate ("kept < total").
SELECT 'wide_in_range_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_wide WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
-- Out-of-range predicate must be dropped ENTIRELY: the Skip block reports 0/N granules kept,
-- which is the strongest signal that the packed minmax is being read correctly.
SELECT 'wide_out_of_range_count', count() FROM t_packed_wide WHERE v = 999999999;
SELECT 'wide_out_of_range_drops_all', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64 = 0
    AND splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64 > 0)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_wide WHERE v = 999999999) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_packed_wide SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_wide;

-- ---------------------------------------------------------------------------------------------
-- Compact part with packed minmax (same checks as wide).
-- ---------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS t_packed_compact;
CREATE TABLE t_packed_compact
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS packed_skip_index_max_bytes = 4194304, index_granularity = 1024;

INSERT INTO t_packed_compact SELECT number, number * 7 FROM numbers(10000);

SELECT 'compact_count', count() FROM t_packed_compact WHERE v BETWEEN 70 AND 700;
SELECT 'compact_secondary_bytes_positive', secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_compact' AND active;
SELECT 'compact_in_range_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_compact WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
SELECT 'compact_out_of_range_count', count() FROM t_packed_compact WHERE v = 999999999;
SELECT 'compact_out_of_range_drops_all', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64 = 0
    AND splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64 > 0)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_compact WHERE v = 999999999) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_packed_compact SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_compact;

-- ---------------------------------------------------------------------------------------------
-- Merge across two INSERTs: the merge writes a new archive from scratch. The post-merge part
-- must serve both row groups via a single skp_idx.packed AND still gate granules on a
-- predicate that doesn't span the whole range. Exercised for both Wide and Compact layouts.
-- ---------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS t_packed_merge_wide;
CREATE TABLE t_packed_merge_wide
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4194304, index_granularity = 1024;

INSERT INTO t_packed_merge_wide SELECT number,         number * 7  FROM numbers(10000);
INSERT INTO t_packed_merge_wide SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'merge_wide_active_parts_before', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge_wide' AND active;
SELECT 'merge_wide_combined_count',  count() FROM t_packed_merge_wide WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_packed_merge_wide FINAL;

SELECT 'merge_wide_active_parts_after', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge_wide' AND active;
SELECT 'merge_wide_count_after',       count() FROM t_packed_merge_wide WHERE v BETWEEN 70 AND 700;
SELECT 'merge_wide_secondary_bytes_positive', secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge_wide' AND active;
SELECT 'merge_wide_index_filters_after', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_merge_wide WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_packed_merge_wide SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_merge_wide;

DROP TABLE IF EXISTS t_packed_merge_compact;
CREATE TABLE t_packed_merge_compact
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS packed_skip_index_max_bytes = 4194304, index_granularity = 1024;

INSERT INTO t_packed_merge_compact SELECT number,         number * 7  FROM numbers(10000);
INSERT INTO t_packed_merge_compact SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'merge_compact_active_parts_before', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge_compact' AND active;
SELECT 'merge_compact_combined_count',  count() FROM t_packed_merge_compact WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_packed_merge_compact FINAL;

SELECT 'merge_compact_active_parts_after', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge_compact' AND active;
SELECT 'merge_compact_count_after',       count() FROM t_packed_merge_compact WHERE v BETWEEN 70 AND 700;
SELECT 'merge_compact_secondary_bytes_positive', secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge_compact' AND active;
SELECT 'merge_compact_index_filters_after', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_merge_compact WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_packed_merge_compact SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_merge_compact;
