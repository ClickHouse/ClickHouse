-- Round-trip test for the packed_skip_index_max_bytes setting: a minmax index written into
-- skp_idx.packed must be queryable (and actually filter granules) across Compact / Wide part
-- layouts and across merges that rewrite the archive. State is checked via system tables only
-- so the test works on any storage (including S3-backed deployments).

-- Wide part with packed minmax.
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
CHECK TABLE t_packed_wide SETTINGS check_query_single_value_result = 1;

-- Compact part with packed minmax.
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
CHECK TABLE t_packed_compact SETTINGS check_query_single_value_result = 1;

-- Out-of-range predicate must be dropped entirely by minmax.
SELECT 'out_of_range_wide', count() FROM t_packed_wide WHERE v = 999999999;

DROP TABLE t_packed_wide;
DROP TABLE t_packed_compact;

-- Second-INSERT + OPTIMIZE FINAL: the merge writes a new archive from scratch. The post-merge
-- part must serve both row groups via a single skp_idx.packed.
DROP TABLE IF EXISTS t_packed_merge;
CREATE TABLE t_packed_merge
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4194304, index_granularity = 1024;

INSERT INTO t_packed_merge SELECT number, number * 7 FROM numbers(10000);
INSERT INTO t_packed_merge SELECT number + 10000, number * 11 FROM numbers(10000);
SELECT 'merge_active_parts_before', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge' AND active;
SELECT 'merge_combined_count', count() FROM t_packed_merge WHERE v BETWEEN 70 AND 700;

OPTIMIZE TABLE t_packed_merge FINAL;

SELECT 'merge_active_parts_after', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge' AND active;
SELECT 'merge_count_after_optimize', count() FROM t_packed_merge WHERE v BETWEEN 70 AND 700;
SELECT 'merge_secondary_bytes_positive', secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed_merge' AND active;
CHECK TABLE t_packed_merge SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_merge;
