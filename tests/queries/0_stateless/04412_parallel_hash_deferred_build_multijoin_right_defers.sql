-- Tags: no-random-settings, no-random-merge-tree-settings
-- The checks depend on the statistics of the tables and on the join order, which randomized settings change.

-- When the right input of a join is another join, its distinct counts are estimates, not `uniq` statistics, so that
-- build defers. Here the tiny `t_s` joins `t_b` into the right input of the join with `t_c`. The nested join reads
-- tables with statistics on both sides and reserves.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET join_algorithm = 'parallel_hash';
SET parallel_hash_join_threshold = 0;
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'true';
SET max_threads = 4;
SET enable_parallel_replicas = 0;

CREATE TABLE t_s (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_b (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_c (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_s SELECT number % 10, number * 3 FROM numbers(1000);
INSERT INTO t_b SELECT number % 30000, number * 5 FROM numbers(300000);
INSERT INTO t_c SELECT number % 30000, number * 7 FROM numbers(300000);

SELECT 'multijoin', count(), sum(cityHash64(s.k, s.v, b.v, c.v))
FROM t_s s INNER JOIN t_b b ON s.k = b.k INNER JOIN t_c c ON b.k = c.k
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'multijoin', count(), sum(cityHash64(s.k, s.v, b.v, c.v))
FROM t_s s INNER JOIN t_b b ON s.k = b.k INNER JOIN t_c c ON b.k = c.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04412 multijoin';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['HashJoinDeferredBuilds'], ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04412 multijoin';
