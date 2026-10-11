-- Tags: no-fasttest
-- no-fasttest: Parquet format is not available in fasttest builds

-- https://github.com/ClickHouse/ClickHouse/issues/113976

SET engine_file_truncate_on_insert = 1;

-- Basic case: Map subcolumns keys/values read directly
INSERT INTO FUNCTION file('04991_parquet_map_subcolumns.parquet')
    SELECT map('v', 'x') AS m FROM numbers(3);

SELECT m.keys, m.values FROM file('04991_parquet_map_subcolumns.parquet');

-- Check if whole-map access working
SELECT m, mapKeys(m), mapValues(m), m['v'] FROM file('04991_parquet_map_subcolumns.parquet');


SELECT m.key FROM file('04991_parquet_map_subcolumns.parquet', Parquet, 'm Array(Tuple(key String, value String))');
SELECT m.value FROM file('04991_parquet_map_subcolumns.parquet', Parquet, 'm Array(Tuple(key String, value String))');

-- Check if real Nested subcolumns working
INSERT INTO FUNCTION file('04991_parquet_nested_subcolumns.parquet')
    SELECT [(number, toString(number))]::Array(Tuple(x UInt64, s String)) AS a FROM numbers(3);

SELECT a.x, a.s FROM file('04991_parquet_nested_subcolumns.parquet');
