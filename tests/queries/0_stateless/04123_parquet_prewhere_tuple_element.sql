-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

SET engine_file_truncate_on_insert = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04123.parquet', Parquet)
SELECT
    number::Int32 AS id,
    (number::Int32, toString(number))::Tuple(a Int32, b String) AS t
FROM numbers(3);

SELECT id, t
FROM file(currentDatabase() || '_04123.parquet', Parquet, 'id Int32, t Tuple(a Int32, b String)')
PREWHERE t.a > 0
ORDER BY id;

SELECT id
FROM file(currentDatabase() || '_04123.parquet', Parquet, 'id Int32, t Tuple(a Int32, b String)')
PREWHERE t.a > 0
ORDER BY id;
