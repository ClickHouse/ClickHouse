-- When the hash table statistics of the build side predict that the runtime filter's Bloom filter would exceed
-- the maximal ratio of set bits, the build is skipped instead of being done and discarded.

DROP TABLE IF EXISTS t_rf_left;
DROP TABLE IF EXISTS t_rf_right;
DROP TABLE IF EXISTS t_rf_left_pair;
DROP TABLE IF EXISTS t_rf_right_pair;
DROP TABLE IF EXISTS t_rf_latest;
CREATE TABLE t_rf_left (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_right (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_left_pair (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_right_pair (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_left SELECT number FROM numbers(100000);
INSERT INTO t_rf_right SELECT number * 2 FROM numbers(2000000);
INSERT INTO t_rf_left_pair SELECT number, 0 FROM numbers(100000);
-- Many distinct pairs, but only 100000 distinct values of `a` and 20 of `b`.
INSERT INTO t_rf_right_pair SELECT number % 100000, intDiv(number, 100000) FROM numbers(2000000);

SET enable_parallel_replicas = 0;
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

-- The statistics keep the maximum key count, so the prediction uses the count of the latest run: 1M build keys
-- saturate the 4 MiB filter at this ratio, half of them would not.
CREATE TABLE t_rf_latest (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_latest SELECT number * 2 FROM numbers(1000000);
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest USING (k);
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest USING (k);
-- 550000 keys, more than half of the kept maximum but too few to saturate the filter: the first run is predicted from
-- the previous count. It spills, so it leaves no count of its own, and the second run takes half of the maximum
-- and gets the filter built. The next two runs are predicted from the count of the second: at this ratio the filter
-- is built (the kept maximum would skip it), at 0.046 it is skipped (half of the maximum would build it).
TRUNCATE TABLE t_rf_latest;
INSERT INTO t_rf_latest SELECT number * 2 FROM numbers(550000);
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest USING (k) SETTINGS max_bytes_before_external_join = 1000000;
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest USING (k);
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest USING (k);
SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest USING (k) SETTINGS join_runtime_bloom_filter_max_ratio_of_set_bits = 0.046;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFiltersCreated'], ProfileEvents['RuntimeFilterBloomFilterBuildsSkipped'] > 0, ProfileEvents['RuntimeFilterRowsChecked'] > 0, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%SELECT count() FROM t_rf_left SEMI LEFT JOIN t_rf_latest%'
    AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_rf_left;
DROP TABLE t_rf_right;
DROP TABLE t_rf_left_pair;
DROP TABLE t_rf_right_pair;
DROP TABLE t_rf_latest;
