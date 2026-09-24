-- `GLOBAL IN` index preparation must preserve the `LowCardinality` columns inside a `Variant`
-- together with their types while converting set elements to the key column types.
-- https://github.com/ClickHouse/ClickHouse/issues/97854
SET enable_analyzer=1;
SET use_index_for_in_with_subqueries = 1;
DROP TABLE IF EXISTS t_variant_lc;
CREATE TABLE t_variant_lc (`id` Decimal(76, 70), `value` Int128) ENGINE = MergeTree ORDER BY (id, value);
INSERT INTO t_variant_lc SELECT number, number FROM numbers(10);
-- Malformed `FixedString` values become `NULL` through `castColumnAccurateOrNull` in index preparation.
-- Runtime membership rejects the `Int128` key because it is not a member of the set's `Variant`.
SELECT id FROM t_variant_lc WHERE (value, id) GLOBAL IN (SELECT toFixedString(toLowCardinality('not a number'), 12), * UNION ALL SELECT DISTINCT toLowCardinality(5), toString(number) FROM numbers(5)); -- { serverError CANNOT_CONVERT_TYPE }
DROP TABLE t_variant_lc;
