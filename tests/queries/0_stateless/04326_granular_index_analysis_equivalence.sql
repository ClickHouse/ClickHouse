-- Tags: no-random-merge-tree-settings
-- Verify that sub-part index analysis (min_marks_per_index_analysis_task > 0)
-- selects exactly the same data as part-level analysis (= 0).

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

-- One large insert (many granules in a single part) plus a few small ones (several parts).
INSERT INTO t_granular_idx SELECT number, number % 1000, toString(number % 50) FROM numbers(500000);
INSERT INTO t_granular_idx SELECT number, number % 1000, toString(number % 50) FROM numbers(1000);
INSERT INTO t_granular_idx SELECT number + 1000000, number % 1000, toString(number % 50) FROM numbers(1000);

-- PK binary search (continuous range on the primary key).
SELECT 'pk_range',
    (SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 SETTINGS min_marks_per_index_analysis_task = 8);

-- PK generic exclusion (non-continuous predicate on the key).
SELECT 'pk_generic',
    (SELECT count() FROM t_granular_idx WHERE k % 7 = 0 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k % 7 = 0 SETTINGS min_marks_per_index_analysis_task = 8);

-- minmax skip index.
SELECT 'minmax',
    (SELECT count() FROM t_granular_idx WHERE v = 42 SETTINGS min_marks_per_index_analysis_task = 0, force_data_skipping_indices = 'idx_v')
  = (SELECT count() FROM t_granular_idx WHERE v = 42 SETTINGS min_marks_per_index_analysis_task = 8, force_data_skipping_indices = 'idx_v');

-- set skip index.
SELECT 'set',
    (SELECT count() FROM t_granular_idx WHERE s = '7' SETTINGS min_marks_per_index_analysis_task = 0, force_data_skipping_indices = 'idx_s')
  = (SELECT count() FROM t_granular_idx WHERE s = '7' SETTINGS min_marks_per_index_analysis_task = 8, force_data_skipping_indices = 'idx_s');

-- Both skip indexes + PK range together.
SELECT 'combined',
    (SELECT count() FROM t_granular_idx WHERE k > 50000 AND v = 42 AND s = '7' SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE k > 50000 AND v = 42 AND s = '7' SETTINGS min_marks_per_index_analysis_task = 8);

-- Empty result.
SELECT 'empty',
    (SELECT count() FROM t_granular_idx WHERE v = 999999 SETTINGS min_marks_per_index_analysis_task = 0)
  = (SELECT count() FROM t_granular_idx WHERE v = 999999 SETTINGS min_marks_per_index_analysis_task = 8);

-- Selected marks: sub-part analysis may prune better (fewer marks) but must not
-- read more marks than part-level analysis, i.e. SelectedMarks(chunked) <= SelectedMarks(base).
-- FORMAT Null so these two queries execute and get logged but emit no rows into the test output.
SET log_queries = 1;
SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 AND v = 42 SETTINGS min_marks_per_index_analysis_task = 0, log_comment = 'gia_base', enable_parallel_replicas = 0 FORMAT Null;
SELECT count() FROM t_granular_idx WHERE k BETWEEN 100000 AND 400000 AND v = 42 SETTINGS min_marks_per_index_analysis_task = 8, log_comment = 'gia_chunked', enable_parallel_replicas = 0 FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT 'marks_le',
    (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_chunked' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1)
  <= (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log WHERE log_comment = 'gia_base' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time DESC LIMIT 1);

DROP TABLE t_granular_idx;
