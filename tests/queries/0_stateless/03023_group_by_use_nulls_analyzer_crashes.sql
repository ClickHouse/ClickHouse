set allow_experimental_analyzer = 1, group_by_use_nulls = 1;

SELECT tuple(tuple(number)) as x FROM numbers(10) GROUP BY (number, tuple(number)) with cube order by x;

select tuple(array(number)) as x FROM numbers(10) GROUP BY number, array(number) WITH ROLLUP order by x;

SELECT tuple(number) AS x FROM numbers(10) GROUP BY GROUPING SETS (number) order by x;

SELECT ignore(toFixedString('Lambda as function parameter', 28), toNullable(28), ignore(8)), sum(marks) FROM system.parts WHERE database = currentDatabase() GROUP BY GROUPING SETS ((2)) FORMAT Null settings optimize_injective_functions_in_group_by=1, optimize_group_by_function_keys=1, group_by_use_nulls=1; -- { serverError ILLEGAL_AGGREGATION }

SELECT toLowCardinality(materialize('a' AS key)),  'b' AS value GROUP BY key WITH CUBE SETTINGS group_by_use_nulls = 1;

SELECT tuple(tuple(number)) AS x
FROM numbers(10)
GROUP BY (number, (toString(x), number))
    WITH CUBE
SETTINGS group_by_use_nulls = 1 FORMAT Null;

SELECT tuple(number + 1) AS x FROM numbers(10) GROUP BY number + 1, toString(x) WITH CUBE settings group_by_use_nulls=1 FORMAT Null;

SELECT tuple(tuple(number)) AS x FROM numbers(10) WHERE toString(toUUID(tuple(number), NULL), x) GROUP BY number, (toString(x), number) WITH CUBE SETTINGS group_by_use_nulls = 1 FORMAT Null;

SELECT  materialize('a'), 'a' AS key GROUP BY key WITH CUBE WITH TOTALS SETTINGS group_by_use_nulls = 1;
