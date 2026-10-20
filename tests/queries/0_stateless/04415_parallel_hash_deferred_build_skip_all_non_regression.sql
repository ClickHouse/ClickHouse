-- `ALL` joins keep every stored block. With a right-side ON filter that most right rows fail, the deferred and the
-- streaming build must agree, with and without a byte limit, and a duplicate-heavy `ALL INNER` join must keep every
-- matching row.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_skip_build;
DROP TABLE IF EXISTS t_skip_probe;
CREATE TABLE t_skip_build (k UInt64, keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_skip_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_skip_build SELECT number, (number < 2000) AS keep, number * 3 FROM numbers(1000000);
INSERT INTO t_skip_probe SELECT number, number * 7 FROM numbers(1000000);

SELECT 'all_left', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 0, join_overflow_mode = 'throw', max_bytes_in_join = 16000000;

SELECT 'all_left', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, join_overflow_mode = 'throw', max_bytes_in_join = 16000000;

SELECT 'all_left_nolimit', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 0;

SELECT 'all_left_nolimit', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 0;

DROP TABLE IF EXISTS t_dup_build;
CREATE TABLE t_dup_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dup_build SELECT number % 10, number FROM numbers(500000);

SELECT 'all_inner_dup', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 0;

SELECT 'all_inner_dup', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_build r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 0;

DROP TABLE t_skip_build;
DROP TABLE t_skip_probe;
DROP TABLE t_dup_build;
