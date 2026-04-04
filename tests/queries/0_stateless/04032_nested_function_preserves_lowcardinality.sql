-- Tags: no-fasttest
-- Verify that the nested() function preserves LowCardinality in column types.
-- See https://github.com/ClickHouse/ClickHouse/issues/95582

SELECT 'nested with LowCardinality(String)';
SELECT toTypeName(nested(['name'], cast(['a', 'b'] as Array(LowCardinality(String))) as a));

SELECT 'nested with Array(LowCardinality(String))';
SELECT toTypeName(nested(['name'], cast([['a', 'b'], ['c']] as Array(Array(LowCardinality(String)))) as a));
