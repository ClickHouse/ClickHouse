-- Concat with Variant type containing LowCardinality caused a LOGICAL_ERROR
-- because convertToFullIfNeeded recursively stripped LowCardinality from inside the Variant column
-- while the type was not updated, creating a type/column mismatch in serialization.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=97581&sha=320e7c9d8876b04a971bd26214b9ac0ab433c250&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%29

SET allow_suspicious_low_cardinality_types = 1;
SET allow_suspicious_types_in_group_by = 1;
SET allow_not_comparable_types_in_comparison_functions = 1;
SET enable_analyzer = 1;

SELECT concat('a', [(1, 2), toLowCardinality(3)]);
SELECT concat(7, [(1., 100000000000000000000.), isNull((NULL, 1048577)), toLowCardinality(toNullable(2))]);
SELECT DISTINCT [CAST('2', 'UInt64')] * 7 FROM numbers(5) WHERE equals(7, number) GROUP BY GROUPING SETS ((isNullable(toLowCardinality(2))), (concat(7, [(1., 100000000000000000000.), isNull((NULL, 1048577)), toLowCardinality(toNullable(2))])), (1)) QUALIFY isNotDistinctFrom(assumeNotNull(2), isNull(2));
