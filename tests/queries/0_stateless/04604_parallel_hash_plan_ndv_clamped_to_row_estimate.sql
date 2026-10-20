-- Tags: no-random-settings, no-random-merge-tree-settings
-- The checks depend on the statistics of the tables and on the join order, which randomized settings change.

-- A limit cuts the rows of the right table, so its `uniq` statistic no longer counts the keys that reach the build: the
-- build defers instead of reserving for the 30000 keys of the whole table.

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
SET enable_hash_join_deferred_build = 1;

CREATE TABLE t_build (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- 30000 distinct keys, each 10 times.
INSERT INTO t_build SELECT number % 30000, number * 3 FROM numbers(300000);
INSERT INTO t_probe SELECT number % 30000, number * 7 FROM numbers(300000);

-- Each of the 100 build rows matches the 10 probe rows of its key, whichever rows the limit keeps.
SELECT 'no_limit', count() FROM t_probe l INNER JOIN t_build r ON l.k = r.k
SETTINGS log_comment = '04604 no_limit';
SELECT 'limit', count() FROM t_probe l INNER JOIN (SELECT k, v FROM t_build LIMIT 100) r ON l.k = r.k
SETTINGS log_comment = '04604 limit';
SELECT 'order_by_limit', count() FROM t_probe l INNER JOIN (SELECT k, v FROM t_build ORDER BY v LIMIT 100) r ON l.k = r.k
SETTINGS log_comment = '04604 order_by_limit';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] BETWEEN 27000 AND 33000,
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04604 %'
ORDER BY log_comment;
