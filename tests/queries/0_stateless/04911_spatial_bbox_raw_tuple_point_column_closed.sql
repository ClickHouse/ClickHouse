-- Regression test: `geoKindNameOfType` (src/Common/GeoBbox.h) reported an EMPTY geometry kind for a
-- plain `Tuple(Float64, Float64)` column, because such a column carries no custom type name. That
-- left one silent-pruning hole after `rejectsColumnGeometryKind`: `spatialBboxIndexValidator` accepts
-- a raw `Tuple(Float64, Float64)` as a supported point column, and `callOnGeometryDataType` resolves
-- exactly that shape as `Point` (it compares the type structurally, so the custom name is irrelevant
-- there). `polygonsIntersectCartesian`/`polygonsWithinCartesian` reject a `Point` argument at every
-- position with `ILLEGAL_TYPE_OF_ARGUMENT`, but with an empty kind name nothing failed closed: a bbox
-- was still derived from the constant polygon, every granule was pruned, and the query answered `0`
-- instead of raising. A `Point`-typed column of the identical shape already failed closed, see
-- 04887_spatial_bbox_column_geometry_kind_mismatch.

DROP TABLE IF EXISTS test_spatial_bbox_raw_tuple_point_column;

CREATE TABLE test_spatial_bbox_raw_tuple_point_column
(
    id UInt32,
    p  Tuple(Float64, Float64),
    INDEX idx_bbox_p p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_raw_tuple_point_column
SELECT number + 1, (500. + number, 500. + number) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_raw_tuple_point_column FINAL;

-- The query polygon is far from the granule's data bbox, so the buggy pruning path skipped the only
-- granule and returned `0` instead of raising.
SELECT count() FROM test_spatial_bbox_raw_tuple_point_column
WHERE polygonsIntersectCartesian(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_raw_tuple_point_column
WHERE polygonsWithinCartesian(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: `pointInPolygon` legitimately accepts a point at its first argument, so a raw
-- `Tuple(Float64, Float64)` column must keep both pruning and evaluation working there.
SELECT count() FROM test_spatial_bbox_raw_tuple_point_column
WHERE pointInPolygon(p, [(499., 499.), (505., 499.), (505., 505.), (499., 505.)]);

SELECT count() FROM test_spatial_bbox_raw_tuple_point_column
WHERE pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]);

DROP TABLE test_spatial_bbox_raw_tuple_point_column;
