-- With `join_overflow_mode = 'throw'`, the deferred build checks `max_rows_in_join` and `max_bytes_in_join` once, after
-- the keys are in the maps, against what the maps hold. So it accepts what the streaming build accepts, and rejects
-- what it rejects. With `break` and a limit, the build does not defer.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET parallel_hash_join_threshold = 0;
SET query_plan_join_swap_table = 'false';
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_build_lim;
DROP TABLE IF EXISTS t_probe_lim;
DROP TABLE IF EXISTS t_dup_lim;

CREATE TABLE t_build_lim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe_lim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_dup_lim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t_build_lim SELECT number, number * 7 FROM numbers(200000);
INSERT INTO t_probe_lim SELECT 100000 + intDiv(number, 2), number * 13 FROM numbers(200000);
-- 50000 rows, but only 10 keys: the limit counts keys.
INSERT INTO t_dup_lim SELECT number % 10, number FROM numbers(50000);

SELECT 'inner_with_limits', count(), sum(cityHash64(l.k, l.v, r.v))
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_rows_in_join = 1000000000, max_bytes_in_join = 1000000000;
SELECT 'inner_with_limits', count(), sum(cityHash64(l.k, l.v, r.v))
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_rows_in_join = 1000000000, max_bytes_in_join = 1000000000,
    log_comment = '04335_on';

SELECT 'dup_rows_below_keys_limit', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_rows_in_join = 1000, join_overflow_mode = 'throw';
SELECT 'dup_rows_below_keys_limit', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_rows_in_join = 1000, join_overflow_mode = 'throw', log_comment = '04335_on';

SELECT count()
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, join_overflow_mode = 'throw', max_rows_in_join = 1000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count()
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, join_overflow_mode = 'throw', max_bytes_in_join = 1024; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'break_succeeds', count() >= 0
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, join_overflow_mode = 'break', max_rows_in_join = 1000, log_comment = '04335_break';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, count(), countIf(ProfileEvents['HashJoinDeferredBuilds'] = 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment IN ('04335_on', '04335_break')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_build_lim;
DROP TABLE t_probe_lim;
DROP TABLE t_dup_lim;
