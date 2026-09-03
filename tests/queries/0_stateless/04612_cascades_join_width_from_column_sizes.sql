-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer.

-- The join output width must come from the columns' real average sizes, not from type defaults.
-- Here the join keeps half the fact rows and its output carries a short `String`; with the real
-- 10-byte size, shuffling the joined rows costs less than shuffling the whole fact input, so the
-- shuffle belongs above the broadcast join. A 64-byte `String` default would inflate the joined
-- rows and put the shuffle below the join instead.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_join_table_stat_hints = '{"t_wd_fact": {"cardinality": 10000000, "avg_row_bytes": 24, "distinct_keys": {"k": 1000000, "g": 10}, "column_bytes": {"k": 8, "g": 8}}, "t_wd_dim": {"cardinality": 5, "avg_row_bytes": 18, "distinct_keys": {"g": 5}, "column_bytes": {"g": 8, "name": 10}}}';

DROP TABLE IF EXISTS t_wd_fact;
DROP TABLE IF EXISTS t_wd_dim;
-- The hints above pin every cost input the plan assertion depends on; the table settings and
-- single-threaded inserts pin the physical layout (one wide part, fixed granularity) so that
-- randomized settings and insert thread scheduling cannot move the plan either.
-- Without `auto_statistics_types = ''` a randomized `materialize_statistics_on_insert` would
-- activate the selectivity estimator, which sees the real tiny tables instead of the hint.
CREATE TABLE t_wd_fact (k UInt64, g UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_wd_dim (g UInt64, name String) ENGINE = MergeTree ORDER BY g
  SETTINGS auto_statistics_types = '', index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_wd_fact SELECT number, number % 10, number FROM numbers(1000) SETTINGS max_insert_threads = 1;
INSERT INTO t_wd_dim SELECT number, concat('nm_', toString(number)) FROM numbers(5) SETTINGS max_insert_threads = 1;

-- The forced shuffle aggregation on `k` needs the joined rows partitioned by `k`; the shuffle
-- above the join moves the estimated 5M narrow joined rows, the one below moves all 10M fact
-- rows, so the plan must place it above (an earlier line than the join in the plan tree).
SELECT 'shuffle above broadcast join',
       minIf(rn, explain LIKE '%ShuffleExchange%') < minIf(rn, explain LIKE '%Broadcast HashJoin%')
FROM (
    SELECT explain, rowNumberInAllBlocks() AS rn FROM (
        EXPLAIN
        SELECT k, sum(v), any(name)
        FROM t_wd_fact AS f
        JOIN t_wd_dim AS d ON f.g = d.g
        GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
                 distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
    )
);

-- The distributed result must match the single-node plan.
SELECT 'distributed', count(), sum(s), min(k), max(k) FROM (
    SELECT k, sum(v) AS s, any(name)
    FROM t_wd_fact AS f
    JOIN t_wd_dim AS d ON f.g = d.g
    GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
             distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0,
             distributed_plan_execute_locally = 1
);
SELECT 'plain', count(), sum(s), min(k), max(k) FROM (
    SELECT k, sum(v) AS s, any(name)
    FROM t_wd_fact AS f
    JOIN t_wd_dim AS d ON f.g = d.g
    GROUP BY k
);

DROP TABLE t_wd_fact;
DROP TABLE t_wd_dim;
