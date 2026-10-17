-- Tags: no-fasttest

SET engine_file_truncate_on_insert = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET allow_suspicious_low_cardinality_types = 1;

INSERT INTO FUNCTION file(
    currentDatabase() || '04307_parquet_dictionary_low_cardinality_fixed.parquet',
    Parquet,
    'x LowCardinality(Int64)')
SELECT toLowCardinality(toInt64(number % 5) - 2) AS x
FROM numbers(20)
SETTINGS output_format_parquet_row_group_size = 20;

SELECT toTypeName(x)
FROM file(
    currentDatabase() || '04307_parquet_dictionary_low_cardinality_fixed.parquet',
    Parquet,
    'x LowCardinality(Int64)')
LIMIT 1;

SELECT count(), sum(x), groupArray(x)
FROM file(
    currentDatabase() || '04307_parquet_dictionary_low_cardinality_fixed.parquet',
    Parquet,
    'x LowCardinality(Int64)');
