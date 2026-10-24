-- Tags: no-fasttest
-- no-fasttest: Parquet

-- An INSERT broken off by `timeout_overflow_mode = 'break'` succeeds, so the file it wrote must be complete:
-- a readable Parquet file holding a contiguous prefix of the source.
-- The source never ends, so the INSERT is always broken off.

INSERT INTO FUNCTION file(currentDatabase() || '_05387.parquet', Parquet, 'x UInt64')
SELECT number AS x FROM system.numbers WHERE sleepEachRow(0.001) = 0
SETTINGS max_block_size = 100, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
    max_execution_time = 2, timeout_overflow_mode = 'break', max_rows_to_read = 0, max_bytes_to_read = 0, engine_file_truncate_on_insert = 1;

SELECT count() > 0, min(x) = 0 AND max(x) + 1 = count() AND uniqExact(x) = count()
FROM file(currentDatabase() || '_05387.parquet', Parquet, 'x UInt64');

INSERT INTO FUNCTION file(currentDatabase() || '_05387_p_{_partition_id}.parquet', Parquet, 'x UInt64') PARTITION BY x % 2
SELECT number AS x FROM system.numbers WHERE sleepEachRow(0.001) = 0
SETTINGS max_block_size = 100, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
    max_execution_time = 2, timeout_overflow_mode = 'break', max_rows_to_read = 0, max_bytes_to_read = 0, engine_file_truncate_on_insert = 1;

SELECT count() > 0, min(x) = 0 AND max(x) + 1 = count() AND uniqExact(x) = count()
FROM file(currentDatabase() || '_05387_p_*.parquet', Parquet, 'x UInt64');

DROP TABLE IF EXISTS a_05387;
DROP TABLE IF EXISTS f_05387;
CREATE TABLE f_05387 (x UInt64) ENGINE = File(Parquet);
CREATE TABLE a_05387 ENGINE = Alias(f_05387);

INSERT INTO a_05387
SELECT number AS x FROM system.numbers WHERE sleepEachRow(0.001) = 0
SETTINGS max_block_size = 100, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
    max_execution_time = 2, timeout_overflow_mode = 'break', max_rows_to_read = 0, max_bytes_to_read = 0, engine_file_truncate_on_insert = 1;

SELECT count() > 0, min(x) = 0 AND max(x) + 1 = count() AND uniqExact(x) = count() FROM f_05387;

DROP TABLE a_05387;
DROP TABLE f_05387;

SET allow_experimental_time_series_table = 1;
DROP TABLE IF EXISTS ts_05387;
CREATE TABLE ts_05387 ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    SAMPLES ENGINE = File(Parquet) TAGS ENGINE = File(Parquet) METRIC FAMILIES ENGINE = File(Parquet);

INSERT INTO ts_05387 (metric_name, tags, samples)
SELECT 'm_05387', map('n', toString(number)), [(toDateTime64(number, 3), toFloat64(number))]
FROM system.numbers WHERE sleepEachRow(0.001) = 0
SETTINGS max_block_size = 100, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
    max_execution_time = 2, timeout_overflow_mode = 'break', max_rows_to_read = 0, max_bytes_to_read = 0, engine_file_truncate_on_insert = 1;

SELECT count() > 0, min(value) = 0 AND max(value) + 1 = count() AND uniqExact(value) = count() FROM timeSeriesSamples(ts_05387);
SELECT count() > 0, countIf(metric_name = 'm_05387') = count() FROM timeSeriesTags(ts_05387);

DROP TABLE ts_05387;
