-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/96912
SET input_format_parquet_use_native_reader_v3 = 1;
INSERT INTO FUNCTION file('04024_data_' || currentDatabase() || '.parquet', Parquet, 'c0 Int32') SELECT 1;
SELECT 1 FROM file('04024_data_' || currentDatabase() || '.parquet', Parquet, 'c0 Int32') PREWHERE c0 = 1;
