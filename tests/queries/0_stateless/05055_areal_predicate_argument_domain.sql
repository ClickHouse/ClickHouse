-- `polygonsIntersectCartesian` and `polygonsWithinCartesian` are defined for areal geometries only:
-- `boost::geometry::intersects` and `within` have no meaning for a `Point` or for the
-- one-dimensional kinds, so both functions refuse them at ANY argument position.
--
-- They used to refuse them while EXECUTING, from inside `callOnTwoGeometryDataTypes`, with
-- `getReturnTypeImpl` returning `UInt8` without looking at its arguments at all. A rejection bound
-- to the data path rather than to the query plan is not observable when nothing is evaluated, so
-- anything that removes every row -- an empty part, a constant-false filter, primary-key or skip
-- index pruning -- turned the exception into a silent `0`.
--
-- `checkArealPredicateArgumentTypes` states that domain once and both functions apply it from
-- `getReturnTypeImpl`, so the rejection happens during analysis, uniformly with every other
-- function. Each query below must raise even though none of them evaluates the predicate on a row.

DROP TABLE IF EXISTS test_areal_predicate_domain;

CREATE TABLE test_areal_predicate_domain (p Point, ls LineString, poly Polygon)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_areal_predicate_domain
VALUES ((0., 0.), [(0., 0.), (1., 1.)], [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]]);

-- Raised from analysis alone: `EXPLAIN` never evaluates the function.
EXPLAIN SELECT polygonsIntersectCartesian(p, poly) FROM test_areal_predicate_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonsWithinCartesian(poly, p) FROM test_areal_predicate_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Refused at either argument position, and for the one-dimensional kinds too.
SELECT polygonsIntersectCartesian(ls, poly) FROM test_areal_predicate_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsIntersectCartesian(poly, ls) FROM test_areal_predicate_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsWithinCartesian(p, poly) FROM test_areal_predicate_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A constant-false filter removes every row, so an execute-time rejection would answer `0` here.
SELECT polygonsIntersectCartesian(p, poly) FROM test_areal_predicate_domain WHERE 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsWithinCartesian(poly, p) FROM test_areal_predicate_domain WHERE 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An empty table removes every row for the same reason.
TRUNCATE TABLE test_areal_predicate_domain;
SELECT polygonsIntersectCartesian(p, poly) FROM test_areal_predicate_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The accepted areal pairs keep working.
INSERT INTO test_areal_predicate_domain
VALUES ((0., 0.), [(0., 0.), (1., 1.)], [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]]);
SELECT polygonsIntersectCartesian(poly, poly) FROM test_areal_predicate_domain;
SELECT polygonsWithinCartesian(poly, poly) FROM test_areal_predicate_domain;

DROP TABLE test_areal_predicate_domain;
