set enable_analyzer=1;
set group_by_use_nulls=1;
set optimize_group_by_function_keys=1;
SELECT ignore(toLowCardinality(number)) FROM numbers(10) GROUP BY GROUPING SETS ((ignore(toLowCardinality(number)), toLowCardinality(number)));
