-- Regression test: `MergeTreeIndexAggregatorSpatialBbox::getGranuleAndReset` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- must fail closed when a granule mixes finite and non-finite (NaN/Inf) geometry values. `BboxAccumulator::valid`
-- (src/Common/GeoBbox.h) is set to false when a non-finite coordinate is seen, but the aggregator only checked
-- `acc.found`, so it still persisted the bbox of the finite subset as a prunable granule -- hiding the
-- `ILLEGAL_TYPE_OF_ARGUMENT` exception that `polygonsIntersectCartesian` raises for the non-finite row.

DROP TABLE IF EXISTS test_spatial_bbox_nonfinite_granule;

CREATE TABLE test_spatial_bbox_nonfinite_granule
(
    id UInt32,
    g  Polygon,
    INDEX idx_bbox g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8;

-- Both rows land in the same granule. Row 1 is finite but far from the query polygon below;
-- row 2 has a NaN vertex.
INSERT INTO test_spatial_bbox_nonfinite_granule VALUES
    (1, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]]),
    (2, [[(nan, nan), (1., 1.), (1., 0.)]]);

OPTIMIZE TABLE test_spatial_bbox_nonfinite_granule FINAL;

-- Query polygon near the origin: far from row 1's finite polygon, so a buggy finite-only bbox
-- would prune the granule instead of raising the exception from row 2's NaN vertex.
SELECT count() FROM test_spatial_bbox_nonfinite_granule
WHERE polygonsIntersectCartesian(g, [[[(0., 0.), (10., 0.), (10., 10.), (0., 10.)]]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE test_spatial_bbox_nonfinite_granule;
