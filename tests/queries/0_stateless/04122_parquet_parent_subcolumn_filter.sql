-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in `fasttest`.

SET enable_analyzer = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04122_parquet_parent_subcolumn_filter.parquet', Parquet)
SELECT
    number::Int32 AS tag,
    (toDate('2024-01-31') + number, toDateTime64('2024-01-31 00:00:00', 6, 'UTC') + number)::Tuple(
        a Nullable(Date32),
        b Nullable(DateTime64(6, 'UTC')))
        AS time_struct
FROM numbers(3);

SELECT time_struct
FROM file(currentDatabase() || '04122_parquet_parent_subcolumn_filter.parquet', Parquet)
WHERE time_struct.a <= toDate('2024-02-01')
ORDER BY ALL
SETTINGS input_format_parquet_use_native_reader_v3 = 1;

SELECT time_struct, time_struct.a
FROM file(currentDatabase() || '04122_parquet_parent_subcolumn_filter.parquet', Parquet)
WHERE time_struct.a <= toDate('2024-02-01')
ORDER BY ALL
SETTINGS input_format_parquet_use_native_reader_v3 = 1;

SELECT *
FROM file(currentDatabase() || '04122_parquet_parent_subcolumn_filter.parquet', Parquet)
WHERE time_struct.a <= toDate('2024-02-01')
ORDER BY ALL
SETTINGS input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04122_parquet_nullable_count.parquet', Parquet)
SELECT
    if(number = 2, CAST(NULL, 'Nullable(Int32)'), number::Int32)::Nullable(Int32) AS a,
    if(number = 1, CAST(NULL, 'Nullable(String)'), toString(number))::Nullable(String) AS b
FROM numbers(3);

-- Force the reader path for a `count` query where the planner may choose
-- a nullable `.null` subcolumn as the smallest input.
SELECT count()
FROM file(currentDatabase() || '04122_parquet_nullable_count.parquet', Parquet)
SETTINGS input_format_parquet_use_native_reader_v3 = 1, optimize_count_from_files = 0;
