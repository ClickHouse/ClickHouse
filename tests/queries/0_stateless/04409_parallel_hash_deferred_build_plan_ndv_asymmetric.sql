-- Tags: no-random-settings, no-random-merge-tree-settings
-- The checks depend on the statistics of the tables and on the join order, which randomized settings change.

-- The build reserves for the distinct keys of its own side. The join order optimization lowers the distinct count of
-- the key in the join's output to that of the side with fewer keys, so it must not size the build: the probe side
-- has 10 keys, the build side 30000.

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
SET query_plan_join_swap_table = 'false';
SET max_threads = 4;
SET enable_parallel_replicas = 0;

CREATE TABLE t_probe_small (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_large (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_probe_small SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_build_large SELECT number % 30000, number FROM numbers(300000);

SELECT 'asym', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_small l INNER JOIN t_build_large r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'asym', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe_small l INNER JOIN t_build_large r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04409 asym';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 27000 AND 33000,
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04409 asym';
