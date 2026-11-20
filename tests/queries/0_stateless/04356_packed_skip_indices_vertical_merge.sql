-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the test fixes the merge-tree configuration (Wide parts, low
-- vertical_merge_algorithm_min_* thresholds) to force a vertical merge; randomized settings
-- would raise those thresholds and silently turn the merge horizontal.
--
-- Regression test for the vertical-merge handoff of packed skip indices. During a vertical
-- merge the horizontal finalizer owns skp_idx.packed, while each per-column writer
-- (MergedColumnOnlyOutputStream) contributes its packed substreams through a borrowed
-- PackedFilesWriter. If that contract regresses, a vertical merge can produce a part with
-- missing or partial packed skip-index data while the horizontal finalizer writes only one
-- archive. The skip indices here are on the gathered (non-sorting-key) columns a and b, so they
-- are written during the vertical per-column phase.
--
-- The merge is forced to MergeAlgorithm::Vertical via enable_vertical_merge_algorithm = 1 and
-- the lowest possible activation thresholds; merge_algorithm is asserted from system.part_log so
-- the test fails (rather than silently going horizontal) if vertical merge stops engaging.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_vert_packed;
CREATE TABLE t_vert_packed
(
    id UInt64,
    a UInt64,
    b UInt64,
    INDEX m_a a TYPE minmax GRANULARITY 1,
    INDEX m_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,             -- Wide, so a and b are gathered separately
         packed_skip_index_max_bytes = '1M',      -- both minmax indices go into skp_idx.packed
         index_granularity = 1024,
         enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vert_packed SELECT number,         number * 7,  number * 11 FROM numbers(10000);
INSERT INTO t_vert_packed SELECT number + 10000, number * 13, number * 17 FROM numbers(10000);

OPTIMIZE TABLE t_vert_packed FINAL;
SYSTEM FLUSH LOGS part_log;

-- The merge actually used the vertical algorithm (otherwise the borrowed-writer path is untested).
SELECT 'merge_algorithm', merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_vert_packed' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Single merged part, both packed indices materialized into its one archive.
SELECT 'active_parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_vert_packed' AND active;
SELECT 'secondary_bytes_positive', secondary_indices_compressed_bytes > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_vert_packed' AND active;

-- Both indices, written by the per-column writers during the vertical phase, still prune granules.
SELECT 'm_a_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_vert_packed WHERE a = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
SELECT 'm_b_filters', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_vert_packed WHERE b = 1100) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;

-- Both predicates return the right rows through the merged packed indices.
SELECT 'm_a_lookup', count() FROM t_vert_packed WHERE a = 700;
SELECT 'm_b_lookup', count() FROM t_vert_packed WHERE b = 1100;

-- Checksums of the merged archive are consistent.
CHECK TABLE t_vert_packed SETTINGS check_query_single_value_result = 1;

DROP TABLE t_vert_packed;
