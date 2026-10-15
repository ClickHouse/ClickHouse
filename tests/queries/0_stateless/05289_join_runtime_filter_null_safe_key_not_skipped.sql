-- https://github.com/ClickHouse/ClickHouse/issues/122167
-- For `ON left.a = right.a AND left.b <=> right.b`, the runtime filter is built on `a` only,
-- but the hash table statistics count distinct `(a, b)` pairs, which are many more than the distinct values of `a`.
-- The filter must still be built and used, not skipped as too dense.

DROP TABLE IF EXISTS t_rf_nse_left;
DROP TABLE IF EXISTS t_rf_nse_right;
CREATE TABLE t_rf_nse_left (a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_nse_right (a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_nse_left SELECT number, 0 FROM numbers(100000);
-- Many distinct pairs, but only 50000 distinct values of `a`.
INSERT INTO t_rf_nse_right SELECT number % 50000, intDiv(number, 50000) FROM numbers(2000000);

SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1, join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 1,
    join_runtime_filter_size_from_hash_table_stats = 1, join_runtime_bloom_filter_max_ratio_of_set_bits = 0.05,
    query_plan_join_swap_table = 0;
-- The hash table statistics are keyed by the join order optimization, which assigns no key when it is disabled.
SET query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;

-- The first run of each query collects the statistics, the second one uses them.
SELECT count() FROM t_rf_nse_left SEMI LEFT JOIN t_rf_nse_right ON t_rf_nse_left.a = t_rf_nse_right.a AND t_rf_nse_left.b <=> t_rf_nse_right.b;
SELECT count() FROM t_rf_nse_left SEMI LEFT JOIN t_rf_nse_right ON t_rf_nse_left.a = t_rf_nse_right.a AND t_rf_nse_left.b <=> t_rf_nse_right.b;
SELECT count() FROM t_rf_nse_left INNER JOIN t_rf_nse_right ON t_rf_nse_left.a = t_rf_nse_right.a AND t_rf_nse_left.b IS NOT DISTINCT FROM t_rf_nse_right.b;
SELECT count() FROM t_rf_nse_left INNER JOIN t_rf_nse_right ON t_rf_nse_left.a = t_rf_nse_right.a AND t_rf_nse_left.b IS NOT DISTINCT FROM t_rf_nse_right.b;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFiltersCreated'], ProfileEvents['RuntimeFilterBloomFilterBuildsSkipped'] > 0, ProfileEvents['RuntimeFilterRowsChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%SELECT count() FROM t_rf_nse_left %JOIN t_rf_nse_right%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_rf_nse_left;
DROP TABLE t_rf_nse_right;
