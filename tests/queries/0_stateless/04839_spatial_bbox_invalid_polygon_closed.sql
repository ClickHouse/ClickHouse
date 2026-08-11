-- Regression test: the spatial_bbox skip index must not hide the "Polygon is not
-- valid" exception. `tryExtractConstGeoBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- builds a pruning bbox directly from a constant polygon literal; if it doesn't
-- validate the ring the same way pointInPolygon validates it at execute time, a
-- self-intersecting polygon whose bbox is disjoint from every granule could prune
-- all of them and return 0 rows silently instead of throwing.

DROP TABLE IF EXISTS test_spatial_bbox_invalid_polygon;

CREATE TABLE test_spatial_bbox_invalid_polygon
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- All points far away from the bowtie polygon's bbox below.
INSERT INTO test_spatial_bbox_invalid_polygon SELECT
    number + 1 AS id,
    (toFloat64((number % 4) + 100), toFloat64((number % 4) + 100)) AS geom
FROM numbers(8);

OPTIMIZE TABLE test_spatial_bbox_invalid_polygon FINAL;

-- Self-intersecting (bowtie) polygon, bbox disjoint from every granule. With
-- validate_polygons = 1 (the default) this must throw, not be pruned away and
-- silently return 0 rows.
SELECT count() FROM test_spatial_bbox_invalid_polygon
WHERE pointInPolygon(geom, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]); -- { serverError BAD_ARGUMENTS }

-- Sanity check: a valid polygon literal must still prune correctly (regression
-- guard for the fix above not disabling pruning wholesale).
SELECT count() FROM test_spatial_bbox_invalid_polygon
WHERE pointInPolygon(geom, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]);

DROP TABLE test_spatial_bbox_invalid_polygon;
