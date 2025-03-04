-- Tags: no-fasttest

set output_format_parquet_use_custom_encoder = 1;
set output_format_parquet_row_group_size = 100000;
set output_format_parquet_data_page_size = 100;
set output_format_parquet_batch_size = 40;
set output_format_parquet_row_group_size_bytes = 1000000000;
set engine_file_truncate_on_insert=1;
set allow_suspicious_low_cardinality_types=1;

-- only scan
-- primitive types
drop table if exists random_data;
CREATE TABLE test_pushdown
(
    id        Int32,
    timestamp DateTime,
    category  String,
    value     Int64,
    is_valid  Bool,
) ENGINE = Memory();

INSERT INTO test_pushdown
SELECT
    number AS id,
    '2024-01-02 00:00:00' - INTERVAL number SECOND AS timestamp,
    ['A', 'B', 'C', 'D'][(number % 4) + 1] AS category,
    randNormal(100, 20) AS value,
    number % 3 != 0 AS is_valid
FROM numbers(100000);

insert into function file('test_pushdown.parquet') select * from test_pushdown order by id;

SELECT count() FROM file(test_pushdown.parquet) WHERE category = 'X' settings input_format_parquet_use_native_reader_v2=true except SELECT count() FROM file(test_pushdown.parquet) WHERE category = 'X';
SELECT sum(value) FROM file(test_pushdown.parquet) WHERE timestamp BETWEEN '2024-01-01 00:00:00' AND '2024-01-02 00:00:00' AND value > 150.0 settings input_format_parquet_use_native_reader_v2=true except SELECT sum(value) FROM file(test_pushdown.parquet) WHERE timestamp BETWEEN '2024-01-01 00:00:00' AND '2024-01-02 00:00:00' AND value > 150.0;
SELECT uniq(category) FROM file(test_pushdown.parquet) WHERE category IN ('A', 'B', 'X') settings input_format_parquet_use_native_reader_v2=true except SELECT uniq(category) FROM file(test_pushdown.parquet) WHERE category IN ('A', 'B', 'X');
SELECT count() FROM file(test_pushdown.parquet) WHERE (category = 'A' OR value < 50) AND is_valid = true settings input_format_parquet_use_native_reader_v2=true except SELECT count() FROM file(test_pushdown.parquet) WHERE (category = 'A' OR value < 50) AND is_valid = true;
