-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer.

-- `cascades_aggregation_pushdown` clones the user's `GROUP BY` step and rebases the clone onto the
-- join keys below the join (`AggregatingStep::rebaseOntoInput`). The clone is an internal
-- aggregation, not the user's `GROUP BY` that `min_rows_per_stream_for_gradual_resize` is
-- documented to affect, so it must keep the strict pre-aggregation resize. The merge above the
-- join is merge-only and never takes the gradual resize either, so the pushed query builds no
-- `GradualResize` at all, while the same query planned without the pushdown does.
-- The pipeline of a locally executed plan fragment is not visible in `EXPLAIN PIPELINE`, hence
-- the introspection through `processors_profile_log`.

DROP TABLE IF EXISTS t_gr_push_facts;
DROP TABLE IF EXISTS t_gr_push_dims;

CREATE TABLE t_gr_push_facts (key UInt32, value Int64) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '', index_granularity = 256;
CREATE TABLE t_gr_push_dims (key UInt32, name String) ENGINE = MergeTree ORDER BY key
    SETTINGS auto_statistics_types = '';
-- A merge between planning and the worker read would invalidate the planned part names.
SYSTEM STOP MERGES t_gr_push_facts;
SYSTEM STOP MERGES t_gr_push_dims;

INSERT INTO t_gr_push_facts SELECT number % 10, number FROM numbers(100000);
INSERT INTO t_gr_push_dims SELECT number, concat('name_', toString(number)) FROM numbers(8);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory, and the
-- number of read streams is capped by the minimum number of marks per concurrent read; either can
-- collapse the pipeline to a single stream and remove every resize processor. Pin both off.
SET max_threads_min_free_memory_per_thread = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;
SET log_processors_profiles = 1;

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_gr_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_gr_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

-- Premise: the aggregation is pushed below the join (a partial `Aggregating` under `JoinLogical`
-- and the merge above it).
SELECT '-- pushed below the join';
SELECT countIf(explain LIKE '%Aggregating%') = 2 AND countIf(explain LIKE '%JoinLogical%') = 1
FROM (EXPLAIN SELECT t1.key AS k, sum(t1.value) AS s FROM t_gr_push_facts AS t1 LEFT JOIN t_gr_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key);

SELECT t1.key AS k, sum(t1.value) AS s FROM t_gr_push_facts AS t1 LEFT JOIN t_gr_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
    FORMAT Null SETTINGS log_comment = '05243_pushed';

-- Positive control: without the distributed planner the user's step stays above the join and
-- takes the gradual resize.
SELECT t1.key AS k, sum(t1.value) AS s FROM t_gr_push_facts AS t1 LEFT JOIN t_gr_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
    FORMAT Null SETTINGS log_comment = '05243_classic', make_distributed_plan = 0, enable_cascades_optimizer = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

SELECT '-- GradualResize built';
-- The system log tables keep merging, so a distributed plan over them fails with `NO_SUCH_DATA_PART`
-- when a part picked by the coordinator is merged away before the worker reads it. Plan locally.
SET make_distributed_plan = 0;
SET enable_cascades_optimizer = 0;
SELECT
    log_comment,
    countIf(name = 'GradualResize') > 0 AS has_gradual_resize
FROM system.processors_profile_log AS p
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05243_pushed', '05243_classic')
) AS q ON p.initial_query_id = q.query_id
WHERE p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_gr_push_facts;
DROP TABLE t_gr_push_dims;
