-- Tags: no-random-settings, no-random-merge-tree-settings
-- The checks depend on the statistics of the tables and on the join order, which randomized settings change.

-- A filter keeps only some keys of the right table, so its `uniq` statistic no longer counts them: the build defers.
-- The same join without the filter reserves from the statistic.

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

CREATE TABLE t_build (k UInt64 STATISTICS(uniq), f UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build SELECT number, number % 2, number * 3 FROM numbers(100000);
INSERT INTO t_probe SELECT number, number * 7 FROM numbers(100000);

SELECT 'filtered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k WHERE r.f = 1
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'filtered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k WHERE r.f = 1
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04411 filtered';

SELECT 'unfiltered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0;
SELECT 'unfiltered', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '04411 unfiltered';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 90000 AND 110000,
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04411 %'
ORDER BY log_comment;
