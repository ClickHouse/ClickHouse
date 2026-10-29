-- The -Tuple combinator takes one Tuple argument per argument of the underlying aggregate
-- function; all tuples must have the same number of elements, and the aggregation at element
-- position i receives the i-th element of every tuple.

SELECT 'two tuples';
SELECT corrTuple((toFloat64(number), toFloat64(number * 2)), (toFloat64(number * 3), toFloat64(100 - number))) FROM numbers(10);
SELECT (round(r.1, 3), round(r.2, 3)) FROM (SELECT avgWeightedTuple((toFloat64(number), toFloat64(number * 10)), (toFloat64(1 + number % 2), toFloat64(1))) AS r FROM numbers(4));

SELECT 'element names come from the first tuple';
SELECT corrTuple(CAST((toFloat64(number), toFloat64(number)), 'Tuple(a Float64, b Float64)'), (toFloat64(number), toFloat64(100 - number))) AS r, toTypeName(r) FROM numbers(5);

SELECT 'state and merge';
SELECT corrTupleMerge(s) FROM (SELECT corrTupleState((toFloat64(number), toFloat64(number * 2)), (toFloat64(number * 3), toFloat64(100 - number))) AS s FROM numbers(10));

SELECT 'composition with -If';
SELECT avgWeightedTupleIf((toFloat64(number), toFloat64(number)), (toFloat64(1), toFloat64(2)), number % 2 = 1) FROM numbers(10);

SELECT 'only-null element in one tuple';
SELECT corrTuple((NULL, toFloat64(number)), (toFloat64(number), toFloat64(100 - number))) FROM numbers(10);

SELECT 'sparse elements with two tuples';
DROP TABLE IF EXISTS test_tuple_multiple_sparse;
CREATE TABLE test_tuple_multiple_sparse (x Tuple(a Int64, b Float64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO test_tuple_multiple_sparse SELECT if(number % 100 = 0, (number, toFloat64(number)), (0, 0.0)) FROM numbers(1000);
SELECT corrTuple(x, x) FROM test_tuple_multiple_sparse;
SELECT corrTuple(x, x) OVER (ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) FROM test_tuple_multiple_sparse ORDER BY x.a DESC LIMIT 1;
DROP TABLE test_tuple_multiple_sparse;

SELECT 'mixed sparse and dense elements with two tuples';
DROP TABLE IF EXISTS test_tuple_multiple_mixed;
CREATE TABLE test_tuple_multiple_mixed (t1 Tuple(v Float64, w Float64), t2 Tuple(v Float64, w Float64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO test_tuple_multiple_mixed SELECT
    (if(cityHash64(number) % 10 = 0, toFloat64(number % 83), 0), toFloat64(1 + number % 3)),
    (toFloat64(1 + number % 7), toFloat64(1 + number % 5))
FROM numbers(1000);
SELECT (round(r.1, 3), round(r.2, 3)) FROM (SELECT avgWeightedTuple(t1, t2) AS r FROM test_tuple_multiple_mixed);
SELECT round(sum(x.1), 3), round(sum(x.2), 3) FROM (SELECT avgWeightedTuple(t1, t2) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS x FROM test_tuple_multiple_mixed);
DROP TABLE test_tuple_multiple_mixed;

SELECT 'errors';
SELECT sumTuple(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT corrTuple((toFloat64(1), toFloat64(2)), tuple(toFloat64(3))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT corrTuple((toFloat64(1), toFloat64(2)), 5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT corrTuple((toFloat64(1), toFloat64(2)), (toFloat64(3), toFloat64(4)), (toFloat64(5), toFloat64(6))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
