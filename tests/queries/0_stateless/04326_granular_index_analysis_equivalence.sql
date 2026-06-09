-- Tags: no-random-merge-tree-settings
-- Sub-part index analysis (min_marks_per_index_analysis_task > 0) must return identical query
-- results to part-level analysis (= 0). The primary-key binary search is boundary-conservative on
-- sub-ranges, so chunking may select a few EXTRA boundary granules (all filtered while reading) -
-- never fewer. Hence query results are asserted equal, and selected marks are asserted within a
-- bounded, one-directional delta.

DROP TABLE IF EXISTS t_granular_idx;

CREATE TABLE t_granular_idx
(
    k UInt64,
    v UInt64,
    s String,
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    INDEX idx_s s TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128;

-- One large part (many granules) plus two small parts.
INSERT INTO t_granular_idx SELECT number, number % 1000, toString(number % 50) FROM numbers(500000);
INSERT INTO t_granular_idx SELECT number, number % 1000, toString(number % 50) FROM numbers(1000);
INSERT INTO t_granular_idx SELECT number + 1000000, number % 1000, toString(number % 50) FROM numbers(1000);

-- Result equivalence: part-level (0) vs heavy chunking (8). Each comparison must print 1.
SELECT 'pk_range',
    (SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 SETTINGS min_marks_per_index_analysis_task = 8);

SELECT 'pk_generic',
    (SELECT count() FROM t_granular_idx WHERE k % 7 = 0 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k % 7 = 0 SETTINGS min_marks_per_index_analysis_task = 8);

SELECT 'minmax',
    (SELECT count() FROM t_granular_idx WHERE v = 42 SETTINGS min_marks_per_index_analysis_task = 0, force_data_skipping_indices = 'idx_v')
  = (SELECT count() FROM t_granular_idx WHERE v = 42 SETTINGS min_marks_per_index_analysis_task = 8, force_data_skipping_indices = 'idx_v');

SELECT 'set',
    (SELECT count() FROM t_granular_idx WHERE s = '7' SETTINGS min_marks_per_index_analysis_task = 0, force_data_skipping_indices = 'idx_s')
  = (SELECT count() FROM t_granular_idx WHERE s = '7' SETTINGS min_marks_per_index_analysis_task = 8, force_data_skipping_indices = 'idx_s');

SELECT 'combined',
    (SELECT count() FROM t_granular_idx WHERE k > 50000 AND v = 42 AND s = '7' SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k > 50000 AND v = 42 AND s = '7' SETTINGS min_marks_per_index_analysis_task = 8);

SELECT 'empty',
    (SELECT count() FROM t_granular_idx WHERE v = 999999 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE v = 999999 SETTINGS min_marks_per_index_analysis_task = 8);

-- Selected-marks bound on a PK-dominated query. Confounding settings are pinned so only
-- min_marks_per_index_analysis_task varies. FORMAT Null executes and logs the query without
-- emitting rows. Chunking must never select fewer marks than the baseline, and the excess is small.
SET log_queries = 1;
SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000
    SETTINGS min_marks_per_index_analysis_task = 0, max_threads_for_indexes = 4,
             use_skip_indexes_on_data_read = 0, secondary_indices_enable_bulk_filtering = 1,
             enable_parallel_replicas = 0, log_comment = 'gia_base' FORMAT Null;
SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000
    SETTINGS min_marks_per_index_analysis_task = 8, max_threads_for_indexes = 4,
             use_skip_indexes_on_data_read = 0, secondary_indices_enable_bulk_filtering = 1,
             enable_parallel_replicas = 0, log_comment = 'gia_chunked' FORMAT Null;
SYSTEM FLUSH LOGS;

SELECT 'marks_bounded',
    (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_chunked' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1)
    >= (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_base' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1)
  AND
    (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_chunked' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1)
    <= (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_base' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1) + 64;

DROP TABLE t_granular_idx;
