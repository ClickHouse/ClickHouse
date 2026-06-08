-- Verify that the nested() function preserves LowCardinality in column types.
-- See https://github.com/ClickHouse/ClickHouse/issues/95582

SET allow_suspicious_low_cardinality_types = 1; -- Required for LowCardinality(UInt32).

SELECT 'nested with LowCardinality(String)';
SELECT toTypeName(nested(['name'], cast(['a', 'b'] as Array(LowCardinality(String))) as a));

SELECT 'nested with LowCardinality(UInt32)';
SELECT toTypeName(nested(['id'], cast([1, 2, 3] as Array(LowCardinality(UInt32))) as a));

SELECT 'nested with Array(LowCardinality(String))';
SELECT toTypeName(nested(['name'], cast([['a', 'b'], ['c']] as Array(Array(LowCardinality(String)))) as a));

SELECT 'nested with mixed LowCardinality and plain columns';
SELECT toTypeName(nested(['a', 'b'], cast(['x', 'y'] as Array(LowCardinality(String))) as a, cast([1, 2] as Array(UInt32)) as b));

SELECT 'nested with plain String (regression check)';
SELECT toTypeName(nested(['name'], cast(['a', 'b'] as Array(String)) as a));

SELECT 'nested with empty arrays';
SELECT toTypeName(nested(['name'], cast([] as Array(LowCardinality(String))) as a));
