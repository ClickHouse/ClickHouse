-- Regression test: `Ring`/`Polygon`/`MultiPolygon` values can legitimately be empty (`[]`).
-- A granule made up only of such empty rows leaves `BboxAccumulator::found == false`
-- (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp), and `MergeTreeIndexGranuleSpatialBbox::serializeBinary`
-- must not throw `LOGICAL_ERROR` in that case -- it must serialize a non-prunable ("no data") granule instead.

DROP TABLE IF EXISTS test_spatial_bbox_empty_geometry;

CREATE TABLE test_spatial_bbox_empty_geometry
(
    id UInt32,
    g  Polygon,
    INDEX idx_bbox g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_spatial_bbox_empty_geometry VALUES (1, []);

SELECT count() FROM test_spatial_bbox_empty_geometry;

DROP TABLE test_spatial_bbox_empty_geometry;
