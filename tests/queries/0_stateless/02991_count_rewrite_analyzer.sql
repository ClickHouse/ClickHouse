-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/59919
SET enable_analyzer=1;

SELECT toTypeName(sum(toNullable('a') IN toNullable('a'))) AS x;
SELECT toTypeName(count(toNullable('a') IN toNullable('a'))) AS x;
SELECT toTypeName(sum(toFixedString('a', toLowCardinality(toNullable(1))) IN toFixedString('a', 1))) AS x;
SELECT toTypeName(count(toFixedString('a', toLowCardinality(toNullable(1))) IN toFixedString('a', 1))) AS x;
