-- Tags: no-fasttest
-- Tests that a column chunk which falls back from dictionary encoding reports its size statistics
-- once, not once per encoding pass.

SET output_format_parquet_max_dictionary_size = 1;
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('05176_fallback.parquet', Parquet)
SELECT toString(number) AS s FROM numbers(1000);

SELECT
    columns.size_statistics.unencoded_byte_array_data_bytes AS reported,
    reported = (SELECT sum(length(toString(number))) FROM numbers(1000)) AS matches_values
FROM file('05176_fallback.parquet', ParquetMetadata)
ARRAY JOIN row_groups
ARRAY JOIN row_groups.columns AS columns;
