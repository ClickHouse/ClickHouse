-- Regression test: the (since removed) multi-argument bbox convention of `pointInPolygon`
-- accepted a depth-1 `Ring` argument in a later position of the variadic `MultiPolygon` form (first
-- polygon argument given as a depth-2 array), treating it as one more `MultiPolygon` component.
-- `getReturnTypeImpl` is stricter: once the first polygon argument is depth-2, every later argument
-- must also be depth-2, otherwise it raises `ILLEGAL_TYPE_OF_ARGUMENT`. Deriving a bbox from such a
-- `Polygon + Ring` call let pruning drop every granule and answer `0`, hiding the exception the query
-- must surface. With `Dynamic`/`Variant` arguments the concrete overload is only built at execution
-- time, so pruning everything away hides the exception completely.

DROP TABLE IF EXISTS test_spatial_bbox_polygon_ring_mix;

CREATE TABLE test_spatial_bbox_polygon_ring_mix
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

-- All rows lie far outside both constant geometries, so a bbox extractor that accepts the
-- `Polygon + Ring` combination would prune every granule and answer `0` instead of raising.
INSERT INTO test_spatial_bbox_polygon_ring_mix SELECT number + 1, (1000. + number, 1000. + number) FROM numbers(8);

SELECT count() FROM test_spatial_bbox_polygon_ring_mix
WHERE pointInPolygon(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]], [(10., 10.), (11., 10.), (11., 11.), (10., 10.)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_polygon_ring_mix
WHERE pointInPolygon(p,
    CAST([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]], 'Polygon')::Dynamic,
    CAST([(10., 10.), (11., 10.), (11., 11.), (10., 10.)], 'Ring')::Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE test_spatial_bbox_polygon_ring_mix;
