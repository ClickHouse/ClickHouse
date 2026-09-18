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
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonAreaCartesian(n) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonPerimeterCartesian(n) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND polygonsDistanceCartesian(poly, CAST(1 AS Nullable(UInt8))) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_geometry_argument_domain
WHERE pointInPolygon((0., 0.), poly) AND length(wkt(n)) > 0
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Raised from analysis alone, with no rows involved at all.
EXPLAIN SELECT polygonAreaCartesian(n) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT wkt(n) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonsUnionCartesian(n, poly) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT wkb(n) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A geometry kind the function refuses, rather than a type it cannot read.
EXPLAIN SELECT polygonAreaCartesian(CAST((0., 0.) AS Point)) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonsUnionCartesian(CAST((0., 0.) AS Point), poly) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonsDistanceCartesian(poly, CAST((0., 0.) AS Point)) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT polygonConvexHullCartesian(CAST((0., 0.) AS Point)) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `wkb` serializes exactly the six named geometry types it has a WKB transform for. `Ring` has no
-- WKB representation, and neither do the anonymous structural types the geometry dispatch otherwise
-- reads, so both are refused -- during analysis, by the same predicate `executeImpl` uses.
EXPLAIN SELECT wkb(CAST([(0., 0.), (1., 1.), (0., 1.)] AS Ring)) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT wkb(CAST([[(0., 0.), (1., 0.), (1., 1.)]] AS Array(Array(Tuple(Float64, Float64))))) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
EXPLAIN SELECT wkb(CAST([(0., 0.), (1., 1.)] AS Array(Tuple(Float64, Float64)))) FROM test_geometry_argument_domain; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A `Variant` argument keeps the uniform `Variant` semantics: an alternative the function refuses
-- is skipped, rather than turning the query into an analysis error. That works only while every
-- rejection is signalled with `ILLEGAL_TYPE_OF_ARGUMENT`, the code `FunctionBaseVariantAdaptor`
-- treats as type incompatibility.
SELECT wkt(CAST((0., 0.)::Point AS Variant(Point, String)));
SELECT round(polygonAreaCartesian(CAST([[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]]::Polygon AS Variant(Polygon, String))), 4);
SELECT polygonsIntersectCartesian(CAST([[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]]::Polygon AS Variant(Polygon, String)), [[(1., 1.), (3., 1.), (3., 3.), (1., 3.)]]::Polygon);
SELECT polygonConvexHullCartesian(CAST([(0., 0.), (2., 0.), (1., 1.)]::Ring AS Variant(Point, Ring)));
SELECT hex(wkb(CAST((0., 0.)::Point AS Variant(Point, String))));

-- The accepted domain keeps working, on real rows.
SELECT round(polygonAreaCartesian(poly), 4) FROM test_geometry_argument_domain;
SELECT round(polygonPerimeterCartesian(poly), 4) FROM test_geometry_argument_domain;
SELECT length(wkt(poly)) > 0 FROM test_geometry_argument_domain;
SELECT polygonsEqualsCartesian(poly, poly) FROM test_geometry_argument_domain;
SELECT length(wkb(poly)) > 0 FROM test_geometry_argument_domain;
SELECT length(wkb([(0., 0.), (1., 1.)]::LineString)) > 0;
SELECT length(wkb([(0., 0.), (1., 1.)]::MultiPoint)) > 0;
SELECT length(wkb([[(0., 0.), (1., 1.)]]::MultiLineString)) > 0;
SELECT length(wkb([[[(0., 0.), (1., 0.), (1., 1.)]]]::MultiPolygon)) > 0;

DROP TABLE test_geometry_argument_domain;
