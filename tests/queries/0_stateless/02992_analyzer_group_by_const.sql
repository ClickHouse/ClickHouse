SET allow_experimental_analyzer=1;

-- Illegal column String of first argument of function concatWithSeparator. Must be a constant String.
SELECT concatWithSeparator('a', 'b') GROUP BY 'a';
-- use-of-uninitialized-value
SELECT concatWithSeparator('|', 'a', concatWithSeparator('|', CAST('a', 'LowCardinality(String)'))) GROUP BY 'a';
SELECT concatWithSeparator('|', 'a', concatWithSeparator('|', CAST('x', 'LowCardinality(String)'))) GROUP BY 'a';
-- should be const like for the query w/o GROUP BY
select dumpColumnStructure('x') GROUP BY 'x';
select dumpColumnStructure('x');
-- from https://github.com/ClickHouse/ClickHouse/pull/60046
SELECT cityHash64('limit', _CAST(materialize('World'), 'LowCardinality(String)')) FROM system.one GROUP BY GROUPING SETS ('limit');

WITH (
        SELECT dummy AS x
        FROM system.one
    ) AS y
SELECT
    y,
    min(dummy)
FROM remote('127.0.0.{1,2}', system.one)
GROUP BY y;

WITH (
        SELECT dummy AS x
        FROM system.one
    ) AS y
SELECT
    y,
    min(dummy)
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY y;
