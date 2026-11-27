-- Tags: no-fasttest
-- no-fasttest: needs Parquet
-- Random settings limits: optimize_arithmetic_operations_in_aggregate_functions=(1, None)

-- `min` / `max` answered from Parquet column chunk statistics (`ParquetReadMinMaxFromStatistics` = 1) must return
-- the same values as reading the data; every other case must read the data (0).

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05331_1.parquet')
SELECT
    number AS i,
    toInt32(toInt64(number) - 5000) AS s,
    number + 9223372036854775808 AS big,
    toInt64(number) - 5000 AS neg,
    toDate('2020-01-01') + number % 3000 AS d,
    toDate32('1960-01-01') + number AS d32,
    toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 AS dt,
    addMilliseconds(toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), number * 13) AS t64,
    toDecimal64(number, 3) / 7 AS dec,
    toDecimal128(number, 10) - 5000 AS dec128,
    if(number % 3 = 0, NULL, toInt64(number) * 2 - 7000) AS n,
    if(number < 3000, NULL, number) AS nn,
    CAST(NULL AS Nullable(Int64)) AS an,
    toString(number) AS str,
    if(number = 5000, nan, toFloat64(number)) AS f,
    toBool(number % 2) AS b,
    toUInt8(2 + number % 2) AS u8
FROM numbers(10000)
SETTINGS output_format_parquet_row_group_size = 1000;

INSERT INTO FUNCTION file(currentDatabase() || '_05331_2.parquet') SELECT number + 20000 AS i FROM numbers(100);
INSERT INTO FUNCTION file(currentDatabase() || '_05331_empty.parquet') SELECT number AS i FROM numbers(0);

DROP TABLE IF EXISTS t_05331_default;
CREATE TABLE t_05331_default (i UInt64, x Int64 DEFAULT 42) ENGINE = File(Parquet);
INSERT INTO t_05331_default (i) SELECT number FROM numbers(1000);

DROP TABLE IF EXISTS t_05331_policy;
CREATE TABLE t_05331_policy (i UInt64) ENGINE = File(Parquet);
INSERT INTO t_05331_policy SELECT number FROM numbers(1000);
DROP ROW POLICY IF EXISTS p_05331 ON t_05331_policy;
CREATE ROW POLICY p_05331 ON t_05331_policy USING i >= 100 AND i < 900 TO ALL;

SET optimize_min_max_from_files = 1;

-- Answered from the statistics.
SELECT min(i), max(i), min(s), max(s), min(big), max(big) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-ints';
SELECT min(d), max(d), min(d32), max(d32), min(dt), max(dt) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'd Date, d32 Date32, dt DateTime(''UTC'')') SETTINGS log_comment = '05331-dates';
SELECT min(t64), max(t64), min(dec), max(dec), min(dec128), max(dec128) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-datetime64_decimal';
SELECT min(n), max(n), min(an), max(an) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-nullable';
SELECT min(i), max(s) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-two_columns';
SELECT min(i), max(i) FROM file(currentDatabase() || '_05331_empty.parquet') SETTINGS log_comment = '05331-empty';
SELECT min(i), max(i) FROM file(currentDatabase() || '_05331_{1,2}.parquet') SETTINGS log_comment = '05331-glob';
SELECT min(s), max(s) FROM file(currentDatabase() || '_05331_{1,2}.parquet', Parquet, 's Int32') SETTINGS log_comment = '05331-missing_column';
SELECT min(i + 1) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-expression';
SELECT min(i), max(i) FROM t_05331_default SETTINGS log_comment = '05331-file_engine';
SELECT min(t64), max(t64), min(dec), max(dec) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 't64 DateTime64(1, ''UTC''), dec Decimal64(1)') SETTINGS log_comment = '05331-rescale';

-- Read: the statistics are not the extrema in the output type, or not of the rows that `min` / `max` see.
SELECT min(str), max(str) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-string';
SELECT min(f), max(f) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-float_nan';
SELECT min(b), max(b) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-bool';
SELECT min(u8), max(u8) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'u8 Bool') SETTINGS log_comment = '05331-uint8_as_bool';
SELECT min(b), max(b) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'b UInt8') SETTINGS log_comment = '05331-bool_as_uint8';
SELECT min(neg), max(neg) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'neg UInt64') SETTINGS log_comment = '05331-int64_as_uint64';
SELECT min(big), max(big) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'big Int64') SETTINGS log_comment = '05331-uint64_as_int64';
SELECT min(i), max(i) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'i UInt8') SETTINGS log_comment = '05331-narrow';
SELECT min(str), max(str) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'str Int64') SETTINGS log_comment = '05331-string_as_int';
SELECT min(nn), max(nn) FROM file(currentDatabase() || '_05331_1.parquet', Parquet, 'nn UInt64') SETTINGS log_comment = '05331-null_as_default';
SELECT min(x), max(x) FROM t_05331_default SETTINGS log_comment = '05331-default_column';
SELECT min(i), max(_file) = currentDatabase() || '_05331_1.parquet' FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-virtual_column';

-- Read: not only `min` / `max` of the rows of the file.
SELECT min(i), max(i) FROM file(currentDatabase() || '_05331_1.parquet') WHERE s > 0 SETTINGS log_comment = '05331-where';
SELECT b, min(i), max(i) FROM file(currentDatabase() || '_05331_1.parquet') GROUP BY b ORDER BY b SETTINGS log_comment = '05331-group_by';
SELECT min(i), count() FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-count';
SELECT minIf(i, s > 0), max(i) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS log_comment = '05331-min_if';
SELECT min(i), max(i) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS aggregate_functions_null_for_empty = 1, log_comment = '05331-null_for_empty';
SELECT min(i), max(i) FROM (SELECT i, arrayJoin(if(i >= 5000, [1], [])) AS z FROM file(currentDatabase() || '_05331_1.parquet')) SETTINGS query_plan_lower_array_join_function = 0, log_comment = '05331-array_join';
SELECT min(i), max(i) FROM t_05331_policy SETTINGS log_comment = '05331-row_policy';
SELECT min(i), max(i) FROM file(currentDatabase() || '_05331_1.parquet') SETTINGS optimize_min_max_from_files = 0, log_comment = '05331-disabled';

SELECT min(i), max(i) FROM fileCluster('test_cluster_two_shards_localhost', currentDatabase() || '_05331_{1,2}.parquet') SETTINGS log_comment = '05331-file_cluster';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetReadMinMaxFromStatistics']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05331-%'
ORDER BY event_time_microseconds;

-- Each file read by the `fileCluster` workers is answered from the statistics.
SELECT sum(ProfileEvents['ParquetReadMinMaxFromStatistics'])
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05331-file_cluster');

DROP ROW POLICY p_05331 ON t_05331_policy;
DROP TABLE t_05331_policy;
DROP TABLE t_05331_default;
