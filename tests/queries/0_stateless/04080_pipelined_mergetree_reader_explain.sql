-- Tags: no-random-merge-tree-settings, no-random-settings

DROP TABLE IF EXISTS t_pipe_explain;

CREATE TABLE t_pipe_explain
(
    key UInt64,
    prewhere_col String,
    rest_col1 String,
    rest_col2 UInt64
)
ENGINE = MergeTree
ORDER BY key;

-- Need enough data to force parallel pool reading (not InOrder single-stream)
INSERT INTO t_pipe_explain
SELECT number, toString(number % 100), 'rest_' || toString(number), number * 10
FROM numbers(1000000);

SELECT '-- Standard reader pipeline';
SET use_pipelined_mergetree_reader = 0;
EXPLAIN PIPELINE
SELECT sum(rest_col2) FROM t_pipe_explain PREWHERE prewhere_col = '42'
SETTINGS max_threads = 2;

SELECT '-- Pipelined reader pipeline';
SET use_pipelined_mergetree_reader = 1;
EXPLAIN PIPELINE
SELECT sum(rest_col2) FROM t_pipe_explain PREWHERE prewhere_col = '42'
SETTINGS max_threads = 2;

SELECT '-- Standard reader: multiple prewhere conditions';
SET use_pipelined_mergetree_reader = 0;
EXPLAIN PIPELINE
SELECT count() FROM t_pipe_explain PREWHERE key > 500000 AND prewhere_col = '50'
SETTINGS max_threads = 2;

SELECT '-- Pipelined reader: multiple prewhere conditions';
SET use_pipelined_mergetree_reader = 1;
EXPLAIN PIPELINE
SELECT count() FROM t_pipe_explain PREWHERE key > 500000 AND prewhere_col = '50'
SETTINGS max_threads = 2;

SELECT '-- Pipelined reader: no prewhere (falls back to standard)';
SET use_pipelined_mergetree_reader = 1;
EXPLAIN PIPELINE
SELECT count() FROM t_pipe_explain WHERE rest_col2 > 5000000
SETTINGS max_threads = 2;

DROP TABLE t_pipe_explain;
