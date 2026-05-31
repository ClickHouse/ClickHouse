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

-- State/Merge roundtrip: sumTupleState -> sumTupleMerge
SELECT 'State/Merge roundtrip';
SELECT sumTupleMerge(s) FROM (SELECT sumTupleState(t) AS s FROM (SELECT tuple(toInt64(1), toFloat64(2.0), toInt64(3)) AS t UNION ALL SELECT tuple(toInt64(4), toFloat64(5.0), toInt64(6)) UNION ALL SELECT tuple(toInt64(7), toFloat64(8.0), toInt64(9))));

-- State/Merge roundtrip with GROUP BY
SELECT 'State/Merge GROUP BY';
SELECT k, sumTupleMerge(s) FROM (SELECT k, sumTupleState(t) AS s FROM (SELECT number % 2 AS k, tuple(toInt64(number), toFloat64(number) * 1.5) AS t FROM numbers(6)) GROUP BY k) GROUP BY k ORDER BY k;

-- State/Merge roundtrip: two-level merge (State -> MergeState -> Merge)
SELECT 'State/Merge two-level';
SELECT sumTupleMerge(s) FROM (SELECT sumTupleMergeState(s) AS s FROM (SELECT sumTupleState(t) AS s FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0))) UNION ALL SELECT sumTupleState(t) AS s FROM (SELECT tuple(toInt64(5), toFloat64(6.0)) AS t UNION ALL SELECT tuple(toInt64(7), toFloat64(8.0)))));

-- StateIf / Merge roundtrip
SELECT 'StateIf/Merge roundtrip';
SELECT sumTupleMerge(s) FROM (SELECT sumTupleStateIf(t, cond) AS s FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1));

-- AggregatingMergeTree roundtrip: persist state to disk and read back
SELECT 'AggregatingMergeTree roundtrip';
DROP TABLE IF EXISTS test_tuple_agg_mt;
CREATE TABLE test_tuple_agg_mt (k UInt8, s AggregateFunction(sumTuple, Tuple(Int64, Float64, Int64))) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO test_tuple_agg_mt SELECT k, sumTupleState(t) AS s FROM (SELECT 1 AS k, tuple(toInt64(10), toFloat64(1.5), toInt64(100)) AS t UNION ALL SELECT 1, tuple(toInt64(20), toFloat64(2.5), toInt64(200)) UNION ALL SELECT 2, tuple(toInt64(30), toFloat64(3.5), toInt64(300))) GROUP BY k;
INSERT INTO test_tuple_agg_mt SELECT k, sumTupleState(t) AS s FROM (SELECT 1 AS k, tuple(toInt64(30), toFloat64(3.5), toInt64(300)) AS t UNION ALL SELECT 2, tuple(toInt64(40), toFloat64(4.5), toInt64(400))) GROUP BY k;
SELECT k, sumTupleMerge(s) FROM test_tuple_agg_mt GROUP BY k ORDER BY k;
DROP TABLE test_tuple_agg_mt;

-- Nullable tuple elements
SELECT 'nullable elements';
SELECT sumTuple(t) FROM (SELECT tuple(toNullable(toInt64(1)), toNullable(toFloat64(2.0))) AS t UNION ALL SELECT tuple(toNullable(toInt64(NULL)), toNullable(toFloat64(4.0))) UNION ALL SELECT tuple(toNullable(toInt64(5)), toNullable(toFloat64(NULL))));
SELECT avgTuple(t) FROM (SELECT tuple(toNullable(toInt64(10)), toNullable(toFloat64(20.0))) AS t UNION ALL SELECT tuple(toNullable(toInt64(NULL)), toNullable(toFloat64(40.0))) UNION ALL SELECT tuple(toNullable(toInt64(30)), toNullable(toFloat64(NULL))));
SELECT minTuple(t) FROM (SELECT tuple(toNullable(toInt64(5)), toNullable(toFloat64(3.0))) AS t UNION ALL SELECT tuple(toNullable(toInt64(NULL)), toNullable(toFloat64(1.0))) UNION ALL SELECT tuple(toNullable(toInt64(2)), toNullable(toFloat64(NULL))));

-- Parametric aggregate function: quantileExactTuple
SELECT 'quantileExactTuple';
SELECT quantileExactTuple(0.5)(t) FROM (SELECT tuple(toFloat64(1.0), toFloat64(10.0)) AS t UNION ALL SELECT tuple(toFloat64(2.0), toFloat64(20.0)) UNION ALL SELECT tuple(toFloat64(3.0), toFloat64(30.0)));
SELECT quantileExactTuple(0.9)(t) FROM (SELECT tuple(toFloat64(1.0), toFloat64(10.0)) AS t UNION ALL SELECT tuple(toFloat64(2.0), toFloat64(20.0)) UNION ALL SELECT tuple(toFloat64(3.0), toFloat64(30.0)));
SELECT quantilesExactTuple(0.25, 0.5, 0.75)(t) FROM (SELECT tuple(toFloat64(number)) AS t FROM numbers(1, 4));

-- Multiple nested combinators: sumTupleIf with Distinct (sumTupleDistinctIf is not valid combinator order, use sumDistinctTupleIf)
-- Note: combinator order matters — Distinct must come before Tuple
SELECT 'multiple nested combinators';
SELECT sumTupleIf(t, cond) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(2.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);
SELECT avgTupleIf(t, cond) FROM (SELECT tuple(toInt64(10), toFloat64(20.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(30), toFloat64(40.0)), 0 UNION ALL SELECT tuple(toInt64(50), toFloat64(60.0)), 1);
SELECT minTupleIf(t, n % 2 = 0) FROM (SELECT tuple(toInt64(number), toFloat64(number) * 1.5) AS t, number AS n FROM numbers(1, 5));
-- State + If + Merge chain
SELECT sumTupleMerge(s) FROM (SELECT sumTupleStateIf(t, cond) AS s FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(2.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1));

-- Multi-argument base function: error because Tuple combinator requires exactly one Tuple argument
SELECT 'multi argument error';
SELECT corrTuple(t1, t2) FROM (SELECT tuple(toFloat64(1.0)) AS t1, tuple(toFloat64(2.0)) AS t2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Error: argument is not a Tuple
SELECT sumTuple(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: array instead of tuple
SELECT sumTuple([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: empty tuple
SELECT sumTuple(tuple()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sparse-serialized tuple elements must not crash with `Bad cast from ColumnSparse`.
SELECT 'sparse tuple';
DROP TABLE IF EXISTS test_tuple_sparse;
CREATE TABLE test_tuple_sparse (x Tuple(Int64, Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO test_tuple_sparse SELECT tuple(0, 0.0) FROM numbers(100);
INSERT INTO test_tuple_sparse SELECT tuple(1, 2.0);
SELECT sumTuple(x) FROM test_tuple_sparse;
SELECT avgTuple(x) FROM test_tuple_sparse;
SELECT minTuple(x) FROM test_tuple_sparse;
SELECT maxTuple(x) FROM test_tuple_sparse;
DROP TABLE test_tuple_sparse;

-- Element of type `Nullable(Nothing)` mixed with a real type: must preserve real-type result and not collapse to all NULLs.
SELECT 'null element with int';
SELECT sumTuple(tuple(NULL, toInt64(1))) AS res, toTypeName(res);
SELECT sumTuple(tuple(toInt64(2), NULL)) AS res, toTypeName(res);
SELECT sumTuple(tuple(NULL, toInt64(1), toFloat64(2.5))) AS res, toTypeName(res);

-- All tuple elements are only-null (`Tuple(Nullable(Nothing))`): the representative nested function
-- collapses to `AggregateFunctionNothing`, so per-element construction must not re-resolve by the
-- placeholder `nothing*` name (which would reject the parameters of parametric aggregates).
SELECT 'all only-null elements';
SELECT groupArrayMovingAvgTuple(2)(tuple(NULL)) AS res, toTypeName(res);
SELECT sumTuple(tuple(NULL)) AS res, toTypeName(res);
SELECT quantileTuple(0.5)(tuple(NULL)) AS res, toTypeName(res);
SELECT sumTuple(tuple(NULL, NULL)) AS res, toTypeName(res);
SELECT avgTuple(tuple(NULL, NULL, NULL)) AS res, toTypeName(res);

-- Parametric aggregate function with `-TupleMerge`: states with different parameter values must be considered same-state.
SELECT 'quantilesTDigestTupleMerge';
SELECT quantilesTDigestTupleMerge(0.9)(s)
FROM
(
    SELECT quantilesTDigestTupleState(0.5)(tuple(toFloat64(number))) AS s
    FROM numbers(10)
);

-- The same with a longer single-element pipeline that exercises parametric `-State`/`-Merge`.
SELECT quantilesTDigestTupleMerge(0.5, 0.9)(s)
FROM
(
    SELECT quantilesTDigestTupleState(0.25)(tuple(toFloat64(number))) AS s
    FROM numbers(100)
);
