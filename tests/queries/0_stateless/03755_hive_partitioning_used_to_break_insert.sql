-- Tags: no-fasttest

-- Test based on https://github.com/ClickHouse/ClickHouse/issues/81048#issuecomment-3139789210

SET use_hive_partitioning=1;

INSERT INTO FUNCTION file(currentDatabase() || '_03755_test_data.csv', 'CSV', 'foo_key String, date Date, bar_key String')
VALUES ('foo', '2019-04-04', 'bar');

INSERT INTO FUNCTION file(currentDatabase() || '_03755_output/date=2019-04-04/path.snappy.parquet', 'Parquet', 'foo_key String, date Date, bar_key String')
SETTINGS use_structure_from_insertion_table_in_table_functions = 0, engine_file_truncate_on_insert = 1
SELECT *
FROM file(currentDatabase() || '_03755_test_data.csv', 'CSV', 'foo_key String, date Date, bar_key String')
SETTINGS use_structure_from_insertion_table_in_table_functions = 0;

SELECT * FROM file(currentDatabase() || '_03755_output/date=2019-04-04/path.snappy.parquet') ORDER BY foo_key;
