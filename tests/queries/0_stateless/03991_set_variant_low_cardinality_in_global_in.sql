-- Regression test: GLOBAL IN with UNION ALL that produces Variant containing LowCardinality
-- caused LOGICAL_ERROR "Bad cast from type DB::ColumnFixedString to DB::ColumnLowCardinality"
-- because convertToFullIfWrapped recursively stripped LowCardinality from ColumnVariant's
-- internal variant columns without updating the corresponding DataTypeVariant, creating
-- column/type position mismatches in KeyCondition::tryPrepareSetColumnsForIndex.
-- https://github.com/ClickHouse/ClickHouse/issues/97854
SET enable_analyzer=1;
DROP TABLE IF EXISTS t_variant_lc;
CREATE TABLE t_variant_lc (`id` Decimal(76, 70), `value` Int128) ENGINE = MergeTree ORDER BY (id, value);
INSERT INTO t_variant_lc SELECT number, number FROM numbers(10);
-- No key type can equal the `Variant(LowCardinality(...), ...)` set element, so key index analysis
-- declines the atom and the error comes from the runtime membership cast instead. That is the code
-- this query returns with no usable key index too.
SELECT id FROM t_variant_lc WHERE (value, id) GLOBAL IN (SELECT toFixedString(toLowCardinality('not a number'), 12), * UNION ALL SELECT DISTINCT toLowCardinality(5), toString(number) FROM numbers(5)); -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t_variant_lc;
