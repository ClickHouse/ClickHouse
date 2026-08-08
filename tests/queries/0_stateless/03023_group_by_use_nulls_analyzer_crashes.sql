set enable_analyzer = 1, group_by_use_nulls = 1;

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

EXPLAIN QUERY TREE
SELECT a, b
FROM numbers(3)
GROUP BY number as a, (number + number) as b WITH CUBE
ORDER BY a, b format Null;

SELECT a, b
FROM numbers(3)
GROUP BY number as a, (number + number) as b WITH CUBE
ORDER BY a, b;

SELECT
    a,
    b,
    cramersVBiasCorrected(a, b)
FROM numbers(3)
GROUP BY
    number AS a,
    number + number AS b
    WITH CUBE
SETTINGS group_by_use_nulls = 1;

SELECT arrayMap(x -> '.', range(number % 10)) AS k FROM remote('127.0.0.{2,3}', numbers(10)) GROUP BY GROUPING SETS ((k)) ORDER BY k settings group_by_use_nulls=1;

SELECT count('Lambda as function parameter') AS c FROM (SELECT ignore(ignore('Lambda as function parameter', 28, 28, 28, 28, 28, 28), 28), materialize('Lambda as function parameter'), 28, 28, 'world', 5 FROM system.numbers WHERE ignore(materialize('Lambda as function parameter'), materialize(toLowCardinality(28)), 28, 28, 28, 28, toUInt128(28)) LIMIT 2) GROUP BY GROUPING SETS ((toLowCardinality(0)), (toLowCardinality(toNullable(28))), (1)) HAVING nullIf(c, 10) < 50 ORDER BY c ASC NULLS FIRST settings group_by_use_nulls=1; -- { serverError ILLEGAL_AGGREGATION }

SELECT arraySplit(x -> 0, []) WHERE materialize(1) GROUP BY (0, ignore('a')) WITH ROLLUP SETTINGS group_by_use_nulls = 1;

SELECT arraySplit(x -> toUInt8(number), []) from numbers(1) GROUP BY toUInt8(number) WITH ROLLUP SETTINGS group_by_use_nulls = 1;

SELECT arraySplit(number -> toUInt8(number), []) from numbers(1) GROUP BY toUInt8(number) WITH ROLLUP SETTINGS group_by_use_nulls = 1;

SELECT count(arraySplit(number -> toUInt8(number), [arraySplit(x -> toUInt8(number), [])])) FROM numbers(10) GROUP BY number, [number] WITH ROLLUP settings group_by_use_nulls=1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT count(arraySplit(x -> toUInt8(number), [])) FROM numbers(10) GROUP BY number, [number] WITH ROLLUP settings group_by_use_nulls=1;
