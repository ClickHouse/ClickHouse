-- Regression test: `MergeTreeIndexAggregatorSpatialBbox::empty` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- is keyed off `acc.found`, which only tracks "found a bbox", not "saw any rows in this granule".
-- A trailing skip-index granule made up only of empty `Ring`/`Polygon`/`MultiPolygon` rows leaves
-- `acc.found == false` even though rows were processed. `MergeTreeDataPartWriterOnDisk::fillSkipIndicesChecksums`
-- only serializes the final, not-yet-flushed granule when the aggregator is non-empty, so this trailing
-- granule's index payload is never written at all -- leaving the on-disk index file short a granule's
-- worth of data and the read path fails with `CANNOT_READ_ALL_DATA` instead of storing a non-prunable granule.

DROP TABLE IF EXISTS test_spatial_bbox_empty_granule_alignment;

CREATE TABLE test_spatial_bbox_empty_granule_alignment
(
    id UInt32,
    g  Polygon,
    INDEX idx_bbox g TYPE spatial_bbox GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- First 8 rows (2 marks) form one full skip-index granule (GRANULARITY 2) with a real, disjoint polygon;
-- it gets flushed by the normal accumulated-marks path in calculateAndSerializeSkipIndices.
INSERT INTO test_spatial_bbox_empty_granule_alignment SELECT number + 1,
    [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(8);
-- Trailing 4 rows (1 mark) never reach the GRANULARITY 2 boundary, so they are only flushed by
-- fillSkipIndicesChecksums at part-write finalization -- and only empty polygons, so acc.found stays false.
INSERT INTO test_spatial_bbox_empty_granule_alignment SELECT number + 9, [] FROM numbers(4);

-- If the trailing granule's flush was skipped, the on-disk index file is short a granule's worth of
-- data and reading it back fails outright instead of correctly treating it as non-prunable.
SELECT count() FROM test_spatial_bbox_empty_granule_alignment
WHERE polygonsIntersectCartesian(g, [[[(0., 0.), (10., 0.), (10., 10.), (0., 10.)]]]);

DROP TABLE test_spatial_bbox_empty_granule_alignment;
