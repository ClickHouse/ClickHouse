-- Every geometry function dispatches on its argument TYPES, via `callOnGeometryDataType` /
-- `callOnTwoGeometryDataTypes`, and raises for a type it cannot read or a geometry kind it refuses.
-- That dispatch used to run from `executeImpl`, which binds the rejection to the data path rather
-- than to the query plan: a `Nullable` argument makes `useDefaultImplementationForNulls` return
-- early for `input_rows_count == 0`, so on a block a sibling conjunct filtered empty the function
-- answered as if the argument had been valid.
--
-- Each function now states its accepted domain from `getReturnTypeImpl`, so the rejection happens
-- during analysis. `EXPLAIN` alone must raise: it never evaluates the function on a row.

DROP TABLE IF EXISTS test_geometry_argument_domain;

CREATE TABLE test_geometry_argument_domain (poly Polygon, n Nullable(UInt8))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO test_geometry_argument_domain
VALUES ([[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], 1);

-- A `Nullable` non-geometry argument: the dispatch cannot read it at all.
-- Each of these answered `0` before, because `pointInPolygon` filters the block empty first.
SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonsUnionCartesian(n, poly).1 IS NOT NULL
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonAreaCartesian(n) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonPerimeterCartesian(n) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonsDistanceCartesian(poly, CAST(1 AS Nullable(UInt8))) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND length(wkt(n)) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError BAD_ARGUMENTS }

-- Raised from analysis alone, with no rows involved at all.
EXPLAIN SELECT polygonAreaCartesian(n) FROM test_geometry_argument_domain; -- { serverError BAD_ARGUMENTS }
EXPLAIN SELECT wkt(n) FROM test_geometry_argument_domain; -- { serverError BAD_ARGUMENTS }
EXPLAIN SELECT polygonsUnionCartesian(n, poly) FROM test_geometry_argument_domain; -- { serverError BAD_ARGUMENTS }

-- A geometry kind the function refuses, rather than a type it cannot read.
EXPLAIN SELECT polygonAreaCartesian(CAST((0., 0.) AS Point)) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonsUnionCartesian(CAST((0., 0.) AS Point), poly) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonsDistanceCartesian(poly, CAST((0., 0.) AS Point)) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonConvexHullCartesian(CAST((0., 0.) AS Point)) FROM test_geometry_argument_domain; -- { serverError BAD_ARGUMENTS }

-- The accepted domain keeps working, on real rows.
SELECT round(polygonAreaCartesian(poly), 4) FROM test_geometry_argument_domain;
SELECT round(polygonPerimeterCartesian(poly), 4) FROM test_geometry_argument_domain;
SELECT length(wkt(poly)) > 0 FROM test_geometry_argument_domain;
SELECT polygonsEqualsCartesian(poly, poly) FROM test_geometry_argument_domain;

DROP TABLE test_geometry_argument_domain;
