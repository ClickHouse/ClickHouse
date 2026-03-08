-- Tags: no-fasttest

-- Test for -Tuple aggregate function combinator

-- Basic: sumTuple
SELECT 'sumTuple';
SELECT sumTuple(t) FROM (SELECT tuple(toInt64(1), toFloat64(2.0), toInt64(3)) AS t UNION ALL SELECT tuple(toInt64(4), toFloat64(5.0), toInt64(6)) UNION ALL SELECT tuple(toInt64(7), toFloat64(8.0), toInt64(9)));

-- Basic: avgTuple
SELECT 'avgTuple';
SELECT avgTuple(t) FROM (SELECT tuple(toInt64(1), toFloat64(2.0), toInt64(3)) AS t UNION ALL SELECT tuple(toInt64(4), toFloat64(5.0), toInt64(6)) UNION ALL SELECT tuple(toInt64(7), toFloat64(8.0), toInt64(9)));

-- Basic: minTuple
SELECT 'minTuple';
SELECT minTuple(t) FROM (SELECT tuple(toInt64(3), toFloat64(5.0), toInt64(9)) AS t UNION ALL SELECT tuple(toInt64(1), toFloat64(2.0), toInt64(6)) UNION ALL SELECT tuple(toInt64(7), toFloat64(8.0), toInt64(3)));

-- Basic: maxTuple
SELECT 'maxTuple';
SELECT maxTuple(t) FROM (SELECT tuple(toInt64(3), toFloat64(5.0), toInt64(9)) AS t UNION ALL SELECT tuple(toInt64(1), toFloat64(2.0), toInt64(6)) UNION ALL SELECT tuple(toInt64(7), toFloat64(8.0), toInt64(3)));

-- GROUP BY
SELECT 'GROUP BY';
SELECT k, sumTuple(t) FROM (SELECT number % 2 AS k, tuple(toInt64(number), toFloat64(number) * 1.5) AS t FROM numbers(6)) GROUP BY k ORDER BY k;

-- Named tuples: names should be preserved
SELECT 'named tuple';
SELECT sumTuple(t) AS res, toTypeName(res) FROM (SELECT tuple(toInt64(1), toFloat64(2.0))::Tuple(a Int64, b Float64) AS t UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0))::Tuple(a Int64, b Float64));

-- Mixed types in tuple
SELECT 'mixed types';
SELECT sumTuple(t) FROM (SELECT tuple(toInt32(1), toFloat32(2.5), toFloat64(3.5)) AS t UNION ALL SELECT tuple(toInt32(4), toFloat32(5.5), toFloat64(6.5)));

-- Single element tuple
SELECT 'single element';
SELECT avgTuple(t) FROM (SELECT tuple(toInt64(10)) AS t UNION ALL SELECT tuple(toInt64(20)) UNION ALL SELECT tuple(toInt64(30)));

-- Table test with numeric types
SELECT 'table numeric';
DROP TABLE IF EXISTS test_tuple_combinator;
CREATE TABLE test_tuple_combinator (k UInt8, t Tuple(Int32, Float64, UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_tuple_combinator VALUES (1, (10, 1.5, 100)), (1, (20, 2.5, 200)), (2, (30, 3.5, 300)), (2, (40, 4.5, 400));
SELECT k, sumTuple(t) FROM test_tuple_combinator GROUP BY k ORDER BY k;
SELECT k, avgTuple(t) FROM test_tuple_combinator GROUP BY k ORDER BY k;
SELECT k, minTuple(t) FROM test_tuple_combinator GROUP BY k ORDER BY k;
SELECT k, maxTuple(t) FROM test_tuple_combinator GROUP BY k ORDER BY k;
DROP TABLE test_tuple_combinator;

-- Combinator chaining: -TupleIf
SELECT 'TupleIf';
SELECT sumTupleIf(t, cond) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);

-- Error: argument is not a Tuple
SELECT sumTuple(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: array instead of tuple
SELECT sumTuple([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
