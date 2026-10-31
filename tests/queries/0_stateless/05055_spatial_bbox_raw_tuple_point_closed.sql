-- Regression test: a bare `Tuple(Float64, Float64)` constant -- not wrapped in `Point`, `Geometry`,
-- `Variant` or `Dynamic` -- must still drive `spatial_bbox` pruning where it is legitimately a point.
--
-- `callOnGeometryDataType` resolves such a type as `Point` by `IDataType::equals`, so the overload
-- that runs for it is the same one a `Point`-named argument gets. `geoKindNameOfType`
-- (`src/Common/GeoBbox.h`) reports no kind name for it, though -- it carries no custom name -- so
-- `extractSpatialPredicateNodeBbox` reads it through `structuralGeoKindName` instead.
--
-- This used to fail closed rather than prune: a bare tuple in an argument position the predicate
-- refuses would have had its `ILLEGAL_TYPE_OF_ARGUMENT` pruned away and the query would have
-- answered `0`. The areal predicates now reject such an argument from `getReturnTypeImpl`, during
-- analysis, where pruning cannot reach it -- which is what the second query below pins -- so the
-- constant is free to prune again. See `05053_spatial_bbox_wasm_const_point_pruning` for the same
-- spelling reaching a WebAssembly UDF's declared `Point` argument.

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

-- A raw-tuple CONSTANT in an argument position `polygonsIntersectCartesian` refuses, alongside a
-- sibling `pointInPolygon` conjunct whose bbox prunes the only granule away. The rejection happens
-- during analysis, so the pruning cannot turn it into a silent `0`.
SELECT count() FROM test_spatial_bbox_raw_tuple_point
WHERE pointInPolygon((0., 0.), poly)
  AND polygonsIntersectCartesian(poly, (500., 500.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `pointInPolygon` accepts a point at its first argument, so the very same raw-tuple spelling must
-- keep pruning there -- refusing to prune for every bare tuple would disable the most common spatial
-- query there is.
SELECT 'const point still prunes', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_raw_tuple_point
      WHERE pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)'), poly))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_raw_tuple_point
WHERE pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)'), poly);

-- And with no `CAST` at all: a bare tuple literal is the spelling almost every such query uses.
SELECT 'bare tuple literal still prunes', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_raw_tuple_point
      WHERE pointInPolygon((0., 0.), poly))
WHERE explain LIKE '%Granules:%';

DROP TABLE test_spatial_bbox_raw_tuple_point;
