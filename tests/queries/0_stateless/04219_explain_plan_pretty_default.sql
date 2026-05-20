-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings, no-random-settings
-- EXPLAIN output may differ

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET optimize_move_to_prewhere = 0;
SET allow_reorder_prewhere_conditions = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET optimize_syntax_fuse_functions = 0;

DROP TABLE IF EXISTS t_explain_default;
CREATE TABLE t_explain_default (a UInt64, b String, c Float64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_explain_default SELECT number, toString(number % 10), number * 1.5 FROM numbers(100);

EXPLAIN
SELECT b, sum(c) AS s, count() AS n, avg(c) AS m
FROM t_explain_default
WHERE a > 10 AND c < 100
GROUP BY b
HAVING s > 50
ORDER BY s DESC
LIMIT 5;

DROP TABLE t_explain_default;