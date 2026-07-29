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

-- Multiple nested combinators. `-Tuple` composes with `-If`, but only in the `<base>TupleIf` order:
-- there `-If` is the outermost combinator, so it consumes the trailing condition argument and passes
-- the single tuple to `-Tuple`. The opposite spelling `<base>IfTuple` makes `-Tuple` the outermost
-- combinator, so it receives both the tuple and the condition and is rejected, because every
-- argument of `-Tuple` must be a `Tuple` (covered by the explicit error test below).
SELECT 'multiple nested combinators';
SELECT sumTupleIf(t, cond) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(2.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);
SELECT avgTupleIf(t, cond) FROM (SELECT tuple(toInt64(10), toFloat64(20.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(30), toFloat64(40.0)), 0 UNION ALL SELECT tuple(toInt64(50), toFloat64(60.0)), 1);
SELECT minTupleIf(t, n % 2 = 0) FROM (SELECT tuple(toInt64(number), toFloat64(number) * 1.5) AS t, number AS n FROM numbers(1, 5));
-- The `<base>IfTuple` order is unsupported: `-Tuple` becomes the outermost combinator and receives both
-- the tuple and the condition, and the trailing condition is not a `Tuple`.
SELECT sumIfTuple(tuple(toInt64(1), toFloat64(2.0)), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- State + If + Merge chain
SELECT sumTupleMerge(s) FROM (SELECT sumTupleStateIf(t, cond) AS s FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(2.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1));

-- Composition with `-Distinct`, which has its own state wrapper and serialization, and composes with `-Tuple`
-- in either order, as well as with `-If`, `-State` and `-Merge`. The two combinator orders mean different things:
--   `sumTupleDistinct` applies `-Distinct` outermost, deduplicating the WHOLE tuple before summing each element;
--   `sumDistinctTuple` applies `-Distinct` per element (inside `-Tuple`), deduplicating EACH element independently.
-- The dataset below has the first element `1` repeated across two distinct tuples `(1, 2.0)` and `(1, 3.0)`, so the
-- two orders produce different first-element sums and the test locks down the intended semantics. With whole-tuple
-- deduplication all four tuples are kept (first elements sum to `1 + 1 + 3 + 5 = 10`), while per-element deduplication
-- collapses the repeated `1` (first elements sum to `1 + 3 + 5 = 9`).
SELECT 'distinct combinators';
SELECT sumTupleDistinct(t) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(3.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);
SELECT sumDistinctTuple(t) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(3.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);
SELECT sumDistinctTupleIf(t, cond) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(3.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);
SELECT avgDistinctTuple(t) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(3.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1);
-- Distinct State/Merge round-trip exercises the `-Distinct` state wrapper and serialization.
SELECT toTypeName(sumDistinctTupleState(t)) FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t UNION ALL SELECT tuple(toInt64(1), toFloat64(3.0)) UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)));
SELECT sumDistinctTupleMerge(s) FROM (SELECT sumDistinctTupleState(t) AS s FROM (SELECT tuple(toInt64(1), toFloat64(2.0)) AS t, 1 AS cond UNION ALL SELECT tuple(toInt64(1), toFloat64(3.0)), 1 UNION ALL SELECT tuple(toInt64(3), toFloat64(4.0)), 0 UNION ALL SELECT tuple(toInt64(5), toFloat64(6.0)), 1));

-- Multi-argument base function: one Tuple per argument of the base function, zipped per element.
SELECT 'multiple tuples';
SELECT corrTuple(t1, t2) FROM (SELECT tuple(toFloat64(number)) AS t1, tuple(toFloat64(100 - number)) AS t2 FROM numbers(10));

-- Error: argument is not a Tuple
SELECT sumTuple(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: array instead of tuple
SELECT sumTuple([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: empty tuple
SELECT sumTuple(tuple()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sparse-serialized tuple elements must not throw a `Bad cast from ColumnSparse` exception.
SELECT 'sparse tuple';
DROP TABLE IF EXISTS test_tuple_sparse;
CREATE TABLE test_tuple_sparse (x Tuple(Int64, Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO test_tuple_sparse SELECT tuple(0, 0.0) FROM numbers(100);
INSERT INTO test_tuple_sparse SELECT tuple(1, 2.0);
SELECT sumTuple(x) FROM test_tuple_sparse;
SELECT (round(r.1, 3), round(r.2, 3)) FROM (SELECT avgTuple(x) AS r FROM test_tuple_sparse);
SELECT minTuple(x) FROM test_tuple_sparse;
SELECT maxTuple(x) FROM test_tuple_sparse;
DROP TABLE test_tuple_sparse;

-- Outer `Nullable(Tuple(...))` argument: routes through the `Null` combinator
-- (`AggregateFunctionNullUnary`), which calls `addBatchSinglePlaceNotNull` / `addBatchSinglePlace`
-- on the nested `-Tuple` function with a null map. Whole-tuple NULLs are skipped; only non-null
-- rows contribute. This exercises the `addBatchSinglePlaceNotNull` override of the `-Tuple` function.
SELECT 'nullable tuple';
SET allow_experimental_nullable_tuple_type = 1;
-- Single place, no NULLs: numbers 0..3 -> first elements sum to 0+1+2+3 = 6, second to 6.0.
SELECT sumTuple(t) FROM (SELECT CAST(tuple(toInt64(number), toFloat64(number)), 'Nullable(Tuple(Int64, Float64))') AS t FROM numbers(4));
-- Single place with NULLs: rows 0 and 3 are NULL and skipped, kept 1,2,4,5 -> sums 1+2+4+5 = 12.
SELECT sumTuple(t) FROM (SELECT if(number % 3 = 0, CAST(NULL, 'Nullable(Tuple(Int64, Float64))'), CAST(tuple(toInt64(number), toFloat64(number)), 'Nullable(Tuple(Int64, Float64))')) AS t FROM numbers(6));
-- `-TupleIf` over `Nullable(Tuple)`: both the if-flag and the null map filter rows. cond = number % 2
-- keeps odd numbers 1,3,5, of which 3 is NULL -> kept 1,5 -> sums 6.
SELECT sumTupleIf(t, cond) FROM (SELECT number % 2 AS cond, if(number % 3 = 0, CAST(NULL, 'Nullable(Tuple(Int64, Float64))'), CAST(tuple(toInt64(number), toFloat64(number)), 'Nullable(Tuple(Int64, Float64))')) AS t FROM numbers(6));
-- GROUP BY over `Nullable(Tuple)` goes through the `Null` wrapper's generic batch loop, which calls
-- the nested `-Tuple` `add` fallback row by row (the wrapper has no keyed batch override):
-- k=0 keeps 2,4 (0 is NULL); k=1 keeps 1,5 (3 is NULL).
SELECT k, sumTuple(t) FROM (SELECT number % 2 AS k, if(number % 3 = 0, CAST(NULL, 'Nullable(Tuple(Int64, Float64))'), CAST(tuple(toInt64(number), toFloat64(number)), 'Nullable(Tuple(Int64, Float64))')) AS t FROM numbers(6)) GROUP BY k ORDER BY k;

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

-- Parametric tuple states that differ only in finalization parameters share one state representation
-- and must resolve to a common type, so `UNION ALL` can unify them. This exercises the
-- `getNormalizedStateType` override (the least-supertype / type-equality path), as opposed to the
-- `-Merge` path above which goes through `haveSameStateRepresentationImpl`. Without parameter
-- normalization the two `AggregateFunction(quantilesTDigestTuple(...), ...)` types would compare as
-- different and there would be no common type, while the non-`Tuple` `quantilesTDigestState` already
-- unifies the same way.
SELECT 'quantilesTDigestTuple common type';
-- The least supertype keeps the first branch's parameter (here `0.5`), like the non-`Tuple` version.
SELECT DISTINCT toTypeName(s)
FROM
(
    SELECT quantilesTDigestTupleState(0.5)(tuple(toFloat64(number))) AS s FROM numbers(10)
    UNION ALL
    SELECT quantilesTDigestTupleState(0.9)(tuple(toFloat64(number))) AS s FROM numbers(10)
);
-- The unified column finalizes end to end across both differently-parameterised branches.
SELECT quantilesTDigestTupleMerge(0.5)(s)
FROM
(
    SELECT quantilesTDigestTupleState(0.5)(tuple(toFloat64(number))) AS s FROM numbers(10)
    UNION ALL
    SELECT quantilesTDigestTupleState(0.9)(tuple(toFloat64(number))) AS s FROM numbers(10)
);
