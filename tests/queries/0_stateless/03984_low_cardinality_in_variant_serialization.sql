-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/97700
-- LowCardinality inside a Variant type could cause a bad cast exception
-- when the column was unwrapped but the data type still expected ColumnLowCardinality.
SET use_variant_as_common_type = 1;

SELECT concat([toLowCardinality(17), 42, 'x'], 'a');
SELECT concat([[toLowCardinality(17), 42]], 'b');
