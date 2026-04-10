-- Verify that skip-index pruning on data read works identically in
-- pipelined and non-pipelined readers.
-- When `use_skip_indexes_on_data_read = 1` (default), skip-index evaluation
-- is deferred from analysis to the data-read phase via `MergeTreeIndexBuildContext`.
-- The pipelined reader must propagate that context so pruning still happens.
-- Tags: no-random-merge-tree-settings, no-parallel-replicas

SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_skip_index_pipelined;

CREATE TABLE t_skip_index_pipelined
(
    key UInt64,
    value UInt64,
    INDEX value_idx value TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
    index_granularity = 64,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

-- 10000 rows, value == key, so minmax index can effectively prune granules.
INSERT INTO t_skip_index_pipelined SELECT number, number FROM numbers(10000);

-- Query that hits a narrow value range: values 500..599 span ~2 granules out of 157.
-- Run with pipelined reader.
SELECT count()
FROM t_skip_index_pipelined
PREWHERE value >= 500 AND value < 600
SETTINGS use_pipelined_mergetree_reader = 1, log_comment = 'skip_idx_pipelined';

-- Same query with standard reader.
SELECT count()
FROM t_skip_index_pipelined
PREWHERE value >= 500 AND value < 600
SETTINGS use_pipelined_mergetree_reader = 0, log_comment = 'skip_idx_standard';

SYSTEM FLUSH LOGS query_log;

-- Both readers must report SelectedMarks (set during data-read skip-index evaluation).
-- Pruning must be effective: SelectedMarks < SelectedMarksTotal.
SELECT
    'pipelined',
    ProfileEvents['SelectedMarks'] AS marks,
    ProfileEvents['SelectedMarksTotal'] AS marks_total,
    marks > 0 AND marks < marks_total AS pruning_effective
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'skip_idx_pipelined';

SELECT
    'standard',
    ProfileEvents['SelectedMarks'] AS marks,
    ProfileEvents['SelectedMarksTotal'] AS marks_total,
    marks > 0 AND marks < marks_total AS pruning_effective
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'skip_idx_standard';

-- Assert: marks selected by both readers are identical.
SELECT
    if(pipelined_marks = standard_marks, 'OK', format('MISMATCH: pipelined={} standard={}', pipelined_marks, standard_marks))
FROM
(
    SELECT ProfileEvents['SelectedMarks'] AS pipelined_marks
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND log_comment = 'skip_idx_pipelined'
) AS p,
(
    SELECT ProfileEvents['SelectedMarks'] AS standard_marks
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND log_comment = 'skip_idx_standard'
) AS s;

DROP TABLE t_skip_index_pipelined;
