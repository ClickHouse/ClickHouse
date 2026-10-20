-- Tags: no-random-settings, no-random-merge-tree-settings
-- The checks depend on the statistics of the tables and on the join order, which randomized settings change.

-- A `uniq` statistic of the right key gives the number of distinct keys, so the build streams and reserves for them
-- instead of deferring. Without a statistic the build defers.

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

CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_uniq (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_nostat (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

-- 30000 distinct keys, each 10 times.
INSERT INTO t_probe SELECT number % 30000, number * 7 FROM numbers(300000);
INSERT INTO t_build_uniq SELECT number % 30000, number * 3 FROM numbers(300000);
INSERT INTO t_build_nostat SELECT number % 30000, number * 3 FROM numbers(300000);

SELECT 'uniq', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build_uniq r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'uniq', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build_uniq r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04407 uniq';

SELECT 'nostat', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build_nostat r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'nostat', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build_nostat r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04407 nostat';

-- Both sides read the same columns of the same table.
SELECT 'self', count(), sum(cityHash64(a.k, a.v, b.v)) FROM t_build_uniq a INNER JOIN t_build_uniq b ON a.k = b.k
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'self', count(), sum(cityHash64(a.k, a.v, b.v)) FROM t_build_uniq a INNER JOIN t_build_uniq b ON a.k = b.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04407 self';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 27000 AND 33000,
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04407 %'
ORDER BY log_comment;
