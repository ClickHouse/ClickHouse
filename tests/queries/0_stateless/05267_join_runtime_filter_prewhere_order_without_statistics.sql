-- Tags: no-openssl-fips
-- no-openssl-fips: `MD5`

-- Enabling join runtime filters must not make a query read more than it reads without them: the runtime filter
-- runs after the query's own PREWHERE conditions also when the table has no statistics (#122403).

SET session_timezone = 'UTC';
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0, join_runtime_filter_min_probe_rows = 0;
SET enable_join_runtime_filters_index_analysis = 0, use_query_condition_cache = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1, enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS t41;
DROP TABLE IF EXISTS t42;

-- No statistics, so PREWHERE conditions are ordered by their size on disk, where the sorted `c1` is smaller than `time`.
CREATE TABLE t41 (`time` DateTime, `c1` String, `s1` AggregateFunction(avg, Decimal(15, 4)), `s2` AggregateFunction(avg, Decimal(15, 4)))
ENGINE = AggregatingMergeTree ORDER BY (c1, time)
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO t41 SELECT toDateTime('2026-01-01 00:00:00') + 60 * ((number * 2654435761) % 525600) AS time, hex(MD5(toString(number % 25000))) AS c1, arrayReduce('avgState', [CAST(number % 1000 AS Decimal(15, 4))]), arrayReduce('avgState', [CAST(number % 777 AS Decimal(15, 4))]) FROM numbers(1000000);
CREATE TABLE t42 (`m` DateTime, `c1` String) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t42 SELECT time, c1 FROM t41 WHERE time IN ('2026-08-07 08:35:00', '2026-08-07 08:47:00', '2026-08-07 09:40:00', '2026-08-08 12:04:00', '2026-08-08 20:04:00', '2026-08-09 11:03:00', '2026-08-09 16:32:00');

-- The reporter's two queries: the limit is met without runtime filters ...
SELECT a.c1, b.m, avgMerge(a.s1), avgMerge(a.s2) FROM t41 AS a INNER JOIN t42 AS b ON (a.time = b.m) AND (a.c1 = b.c1) WHERE a.time IN ('2026-08-07 08:35:00', '2026-08-07 08:47:00', '2026-08-07 09:40:00', '2026-08-08 12:04:00', '2026-08-08 20:04:00', '2026-08-09 11:03:00', '2026-08-09 16:32:00') GROUP BY a.c1, b.m ORDER BY a.c1, b.m
SETTINGS join_algorithm = 'grace_hash', max_bytes_to_read = '22500000', enable_join_runtime_filters = 0, log_comment = 'rf_off' FORMAT Null;
-- ... and with them.
SELECT a.c1, b.m, avgMerge(a.s1), avgMerge(a.s2) FROM t41 AS a INNER JOIN t42 AS b ON (a.time = b.m) AND (a.c1 = b.c1) WHERE a.time IN ('2026-08-07 08:35:00', '2026-08-07 08:47:00', '2026-08-07 09:40:00', '2026-08-08 12:04:00', '2026-08-08 20:04:00', '2026-08-09 11:03:00', '2026-08-09 16:32:00') GROUP BY a.c1, b.m ORDER BY a.c1, b.m
SETTINGS join_algorithm = 'grace_hash', max_bytes_to_read = '22500000', enable_join_runtime_filters = 1, log_comment = 'rf_on';

-- With reordering disabled, the runtime filter must not be the one condition moved to PREWHERE.
SELECT a.c1, b.m, avgMerge(a.s1), avgMerge(a.s2) FROM t41 AS a INNER JOIN t42 AS b ON (a.time = b.m) AND (a.c1 = b.c1) WHERE a.time IN ('2026-08-07 08:35:00', '2026-08-07 08:47:00', '2026-08-07 09:40:00', '2026-08-08 12:04:00', '2026-08-08 20:04:00', '2026-08-09 11:03:00', '2026-08-09 16:32:00') GROUP BY a.c1, b.m ORDER BY a.c1, b.m
SETTINGS join_algorithm = 'grace_hash', max_bytes_to_read = '22500000', enable_join_runtime_filters = 1, allow_reorder_prewhere_conditions = 0, move_all_conditions_to_prewhere = 0, log_comment = 'rf_c' FORMAT Null;

-- LEFT ANTI JOIN on a Nullable key, whose runtime filter also passes NULL keys.
SELECT count() FROM t41 AS a LEFT ANTI JOIN t42 AS b ON toNullable(a.c1) = b.c1 WHERE a.time IN ('2026-08-07 08:35:00', '2026-08-07 08:47:00', '2026-08-07 09:40:00', '2026-08-08 12:04:00', '2026-08-08 20:04:00', '2026-08-09 11:03:00', '2026-08-09 16:32:00')
SETTINGS join_algorithm = 'hash', max_bytes_to_read = '22500000', enable_join_runtime_filters = 1, log_comment = 'rf_d';

SYSTEM FLUSH LOGS query_log;
-- Every query that enables runtime filters must have checked rows with them, and the one that disables them must not.
SELECT if(countIf(log_comment = 'rf_off') = 1 AND countIf(log_comment = 'rf_on') = 1
        AND countIf(log_comment = 'rf_c') = 1 AND countIf(log_comment = 'rf_d') = 1
        AND maxIf(read_bytes, log_comment = 'rf_on') <= maxIf(read_bytes, log_comment = 'rf_off') * 1.1
        AND maxIf(ProfileEvents['RuntimeFilterRowsChecked'], log_comment = 'rf_off') = 0
        AND minIf(ProfileEvents['RuntimeFilterRowsChecked'], log_comment IN ('rf_on', 'rf_c', 'rf_d')) > 0,
    'Ok',
    format('read_bytes with runtime filters {}, without {}; RuntimeFilterRowsChecked off {}, on {}, arm C {}, arm D {}',
        maxIf(read_bytes, log_comment = 'rf_on'), maxIf(read_bytes, log_comment = 'rf_off'),
        maxIf(ProfileEvents['RuntimeFilterRowsChecked'], log_comment = 'rf_off'),
        maxIf(ProfileEvents['RuntimeFilterRowsChecked'], log_comment = 'rf_on'),
        maxIf(ProfileEvents['RuntimeFilterRowsChecked'], log_comment = 'rf_c'),
        maxIf(ProfileEvents['RuntimeFilterRowsChecked'], log_comment = 'rf_d')))
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('rf_off', 'rf_on', 'rf_c', 'rf_d');

DROP TABLE t41;
DROP TABLE t42;
