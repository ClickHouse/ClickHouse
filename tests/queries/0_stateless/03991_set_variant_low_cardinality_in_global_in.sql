-- `GLOBAL IN` index preparation must preserve the `LowCardinality` columns inside a `Variant`
-- together with their types while converting set elements to the key column types: stripping
-- `LowCardinality` from the variant columns without updating the `Variant` type would make the
-- columns and the types in `KeyCondition::tryPrepareSetColumnsForIndex` disagree.
-- https://github.com/ClickHouse/ClickHouse/issues/97854
SET use_index_for_in_with_subqueries = 1;
DROP TABLE IF EXISTS t_variant_lc;
CREATE TABLE t_variant_lc (`id` Decimal(76, 70), `value` Int128) ENGINE = MergeTree ORDER BY (id, value);
INSERT INTO t_variant_lc SELECT number, number FROM numbers(10);
-- The lookup cannot cast the `Int128` key into the `Variant` of the set, so the index does not use the set,
-- and the query fails as a full scan does.
SELECT id FROM t_variant_lc WHERE (value, id) GLOBAL IN (SELECT toFixedString(toLowCardinality('not a number'), 12), * UNION ALL SELECT DISTINCT toLowCardinality(5), toString(number) FROM numbers(5)); -- { serverError CANNOT_CONVERT_TYPE }
-- With `Int128` among the alternatives, index analysis casts the set into the key type, the values of the
-- `LowCardinality` alternative included.
SELECT value FROM t_variant_lc WHERE value GLOBAL IN (SELECT toFixedString(toLowCardinality('7'), 12) UNION ALL SELECT toInt128(5));
DROP TABLE t_variant_lc;
