SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_stream_pr;
CREATE TABLE t_stream_pr (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_stream_pr SELECT number FROM numbers(100);

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM t_stream_pr STREAM
PREWHERE (x IN (t)) OR (x IN (t))
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_stream_pr;
