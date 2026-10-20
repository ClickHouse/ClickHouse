-- `RIGHT` and `FULL` joins must keep their non-joined right rows: those with a NULL key and those that fail the ON
-- condition. The deferred build must give the same rows as the streaming build.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_rf_build;
DROP TABLE IF EXISTS t_rf_probe;
CREATE TABLE t_rf_build (k Nullable(UInt64), keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_rf_build SELECT if(number % 7 = 0, NULL, number), (number < 2000) AS keep, number * 3 FROM numbers(300000);
INSERT INTO t_rf_probe SELECT number, number * 7 FROM numbers(300000);

SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l RIGHT JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 0;

SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l RIGHT JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 0;

SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l FULL JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 0;

SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l FULL JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 0;

DROP TABLE t_rf_build;
DROP TABLE t_rf_probe;
