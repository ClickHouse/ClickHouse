-- When the hash table statistics of the build side predict that the runtime filter's Bloom filter would exceed
-- the maximal ratio of set bits, the build is skipped instead of being done and discarded.

DROP TABLE IF EXISTS t_rf_left;
DROP TABLE IF EXISTS t_rf_right;
DROP TABLE IF EXISTS t_rf_left_pair;
DROP TABLE IF EXISTS t_rf_right_pair;
CREATE TABLE t_rf_left (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_right (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_left_pair (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_right_pair (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_left SELECT number FROM numbers(100000);
INSERT INTO t_rf_right SELECT number * 2 FROM numbers(2000000);
INSERT INTO t_rf_left_pair SELECT number, 0 FROM numbers(100000);
-- Many distinct pairs, but only 100000 distinct values of `a` and 20 of `b`.
INSERT INTO t_rf_right_pair SELECT number % 100000, intDiv(number, 100000) FROM numbers(2000000);

SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1, join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 1,
    join_runtime_filter_size_from_hash_table_stats = 1, join_runtime_bloom_filter_max_ratio_of_set_bits = 0.05,
    query_plan_join_swap_table = 0;
-- The hash table statistics are keyed by the join order optimization, which assigns no key when it is disabled.
SET query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;

-- The first run has no statistics: the filter is built and discarded as too dense. The second run has them.
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_right USING (k);
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_right USING (k);

-- The statistics count distinct pairs, which only bounds the distinct values of the per-column filters:
-- those are sized by the count, but never skipped.
SELECT count() FROM t_rf_left_pair SEMI LEFT JOIN t_rf_right_pair USING (a, b);
SELECT count() FROM t_rf_left_pair SEMI LEFT JOIN t_rf_right_pair USING (a, b);

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFiltersCreated'], ProfileEvents['RuntimeFilterBloomFilterBuildsSkipped'] > 0, ProfileEvents['RuntimeFilterRowsChecked'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%SELECT count() FROM t_rf_left% SEMI LEFT JOIN t_rf_right%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_rf_left;
DROP TABLE t_rf_right;
DROP TABLE t_rf_left_pair;
DROP TABLE t_rf_right_pair;
