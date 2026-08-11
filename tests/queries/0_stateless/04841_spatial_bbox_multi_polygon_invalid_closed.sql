-- Regression test: `MergeTreeIndexConditionSpatialBbox::extractQueryBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- must fail closed for the multi-argument `pointInPolygon(geom, poly1, poly2, ...)` form when any
-- constant polygon argument is invalid. Silently skipping the invalid one and building the query
-- bbox from the remaining valid polygons can prune away granules that should have been left for
-- `pointInPolygon` to reject with an exception at execute time.

DROP TABLE IF EXISTS test_spatial_bbox_multi_polygon_invalid;

CREATE TABLE test_spatial_bbox_multi_polygon_invalid
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- All points near (0, 0), far from the second (valid) polygon around (50, 50).
INSERT INTO test_spatial_bbox_multi_polygon_invalid SELECT
    number + 1 AS id,
    (toFloat64(number % 4) / 10, toFloat64(number % 4) / 10) AS geom
FROM numbers(8);

OPTIMIZE TABLE test_spatial_bbox_multi_polygon_invalid FINAL;

-- First polygon is a self-intersecting bowtie (invalid); second is valid but far away.
-- With validate_polygons = 1 (the default) this must throw, not be pruned to 0 rows.
SELECT count() FROM test_spatial_bbox_multi_polygon_invalid
WHERE pointInPolygon(
    geom,
    [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)],
    [(50., 50.), (55., 50.), (55., 55.), (50., 55.), (50., 50.)]
); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_multi_polygon_invalid;
