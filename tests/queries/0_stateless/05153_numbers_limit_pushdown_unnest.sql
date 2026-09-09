-- `arrayJoin` changes the number of rows after the source has been read, so the outer `LIMIT` must
-- not be pushed down into a `numbers`-like source. The guard used to compare the function name
-- literally, which missed the case-insensitive `unnest` alias whenever the AST was not normalized
-- (`normalize_function_names = 0`): the source was then truncated before the arrays were expanded
-- and the query returned too few rows.

SELECT 'unnest, not normalized';
SELECT unnest(if(number < 3, [], [number])) AS x FROM numbers(100) LIMIT 3 SETTINGS normalize_function_names = 0;

SELECT 'unnest, normalized';
SELECT unnest(if(number < 3, [], [number])) AS x FROM numbers(100) LIMIT 3 SETTINGS normalize_function_names = 1;

SELECT 'UNNEST, not normalized';
SELECT UNNEST(if(number < 3, [], [number])) AS x FROM numbers(100) LIMIT 3 SETTINGS normalize_function_names = 0;

SELECT 'arrayJoin';
SELECT arrayJoin(if(number < 3, [], [number])) AS x FROM numbers(100) LIMIT 3 SETTINGS normalize_function_names = 0;

SELECT 'generate_series';
SELECT unnest(if(generate_series < 3, [], [generate_series])) AS x FROM generate_series(0, 99) LIMIT 3 SETTINGS normalize_function_names = 0;

-- The same invariant when `arrayJoin` is hidden inside a SQL UDF that is inlined later.
SELECT 'sql udf';
DROP FUNCTION IF EXISTS udf_05153_array_join;
CREATE FUNCTION udf_05153_array_join AS x -> arrayJoin(x);
SELECT udf_05153_array_join(if(number < 3, [], [number])) AS x FROM numbers(100) LIMIT 3;
DROP FUNCTION udf_05153_array_join;
