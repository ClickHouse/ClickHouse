-- Regression test: a bare `Tuple(Float64, Float64)` argument -- not wrapped in `Point`,
-- `Geometry`, `Variant` or `Dynamic` -- must fail `spatial_bbox` pruning closed for any argument
-- position the predicate has not declared as a point position.
--
-- `callOnGeometryDataType` resolves such a type as `Point` by `IDataType::equals`, so
-- `polygonsIntersectCartesian` raises `ILLEGAL_TYPE_OF_ARGUMENT` for it while executing.
-- `constGeoKindName` (`src/Common/GeoBbox.h`) reports no kind name for it, though, so
-- `extractSpatialPredicateNodeBbox` used to accept the argument as harmless: a constant polygon in
-- the same call, or a sibling conjunct's bbox, could then prune every granule away and the query
-- answered `0` instead of raising.
--
-- The check is the predicate's own `treatsConstTupleAsPoint(arg_index)`: a bare two-`Float64`
-- `Tuple` is legitimate exactly where the predicate says a point belongs (`pointInPolygon`'s first
-- argument, a WebAssembly UDF argument declared `Point`), and fails closed everywhere else. See
-- `05053_spatial_bbox_wasm_const_point_pruning` for the positive half.

DROP TABLE IF EXISTS test_spatial_bbox_raw_tuple_point;

CREATE TABLE test_spatial_bbox_raw_tuple_point
(
    p    Tuple(Float64, Float64),
    poly Polygon,
    INDEX idx_bbox_p p TYPE spatial_bbox GRANULARITY 1,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- Every granule lies near (100, 100), far from the constants below, so a bbox derived from any of
-- them prunes the only granule and hides the exception.
INSERT INTO test_spatial_bbox_raw_tuple_point
SELECT (100., 100.), [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]] FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- The indexed raw-tuple COLUMN as the geometry argument: the runtime reads it as a `Point`, which
-- `polygonsIntersectCartesian` refuses. The assertion is the exception itself rather than a granule
-- count, since the constant is rejected while the plan's header is computed on a zero-row block
-- (see `05051_spatial_bbox_empty_geometry_no_validate_pruning`).
SELECT count() FROM test_spatial_bbox_raw_tuple_point
WHERE polygonsIntersectCartesian(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A raw-tuple CONSTANT in a sibling `AND` conjunct, which used to be downgraded to
-- `NoInfo`/`NotApplicable` and let the `pointInPolygon` conjunct prune the exception away.
SELECT count() FROM test_spatial_bbox_raw_tuple_point
WHERE pointInPolygon((0., 0.), poly)
  AND polygonsIntersectCartesian(poly, (500., 500.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `pointInPolygon`'s FIRST argument is declared a point position, so the very same raw-tuple
-- spelling must keep pruning there -- failing closed for every bare tuple would disable the most
-- common spatial query there is.
SELECT 'const point still prunes', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_raw_tuple_point
      WHERE pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)'), poly))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_raw_tuple_point
WHERE pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)'), poly);

DROP TABLE test_spatial_bbox_raw_tuple_point;
