-- `registerStorageMergeTree.cpp` and `MergeTreeIndices.cpp` advertise `Ring` and `MultiPolygon` as
-- supported indexed column shapes for `TYPE spatial_bbox`, but the rest of the suite only covers
-- `Point`, `Polygon` and raw `Tuple(Float64, Float64)` columns. `Ring` and `MultiPolygon` sit at
-- different `ColumnArray` nesting depths than `Polygon`, and the accumulator walks that nesting
-- recursively (`MergeTreeIndexSpatialBbox.cpp`), so each depth needs its own happy-path proof.
--
-- Each column is checked twice: with a query bbox that must KEEP the near granule and drop the far
-- one, and with a query bbox far from every row that must drop both. Checking only the second case
-- would let a regression that prunes everything pass.

DROP TABLE IF EXISTS test_spatial_bbox_ring;
DROP TABLE IF EXISTS test_spatial_bbox_multipolygon;

SET optimize_move_to_prewhere = 0;

-- `Ring` = Array(Point): one `ColumnArray` level above `Point`.
CREATE TABLE test_spatial_bbox_ring
(
    id UInt32,
    r  Ring,
    INDEX idx_bbox_r r TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO test_spatial_bbox_ring VALUES (1, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]), (2, [(0.5, 0.5), (2., 0.5), (2., 2.), (0.5, 2.), (0.5, 0.5)]);
INSERT INTO test_spatial_bbox_ring VALUES (3, [(100., 100.), (101., 100.), (101., 101.), (100., 101.), (100., 100.)]), (4, [(102., 102.), (103., 102.), (103., 103.), (102., 103.), (102., 102.)]);

SELECT 'ring rows near', id FROM test_spatial_bbox_ring
WHERE polygonsIntersectCartesian(r, [[(-1., -1.), (3., -1.), (3., 3.), (-1., 3.), (-1., -1.)]])
ORDER BY id;

SELECT 'ring near', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_ring
      WHERE polygonsIntersectCartesian(r, [[(-1., -1.), (3., -1.), (3., 3.), (-1., 3.), (-1., -1.)]]))
WHERE explain LIKE '%Granules:%';

SELECT 'ring far', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_ring
      WHERE polygonsIntersectCartesian(r, [[(500., 500.), (501., 500.), (501., 501.), (500., 501.), (500., 500.)]]))
WHERE explain LIKE '%Granules:%';

SELECT 'ring rows far', count() FROM test_spatial_bbox_ring
WHERE polygonsIntersectCartesian(r, [[(500., 500.), (501., 500.), (501., 501.), (500., 501.), (500., 500.)]]);

-- `MultiPolygon` = Array(Polygon): two `ColumnArray` levels above `Ring`.
CREATE TABLE test_spatial_bbox_multipolygon
(
    id UInt32,
    mp MultiPolygon,
    INDEX idx_bbox_mp mp TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO test_spatial_bbox_multipolygon VALUES (1, [[[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]]), (2, [[[(0.5, 0.5), (2., 0.5), (2., 2.), (0.5, 2.), (0.5, 0.5)]]]);
INSERT INTO test_spatial_bbox_multipolygon VALUES (3, [[[(100., 100.), (101., 100.), (101., 101.), (100., 101.), (100., 100.)]]]), (4, [[[(102., 102.), (103., 102.), (103., 103.), (102., 103.), (102., 102.)]]]);

SELECT 'multipolygon rows near', id FROM test_spatial_bbox_multipolygon
WHERE polygonsIntersectCartesian(mp, [[(-1., -1.), (3., -1.), (3., 3.), (-1., 3.), (-1., -1.)]])
ORDER BY id;

SELECT 'multipolygon near', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_multipolygon
      WHERE polygonsIntersectCartesian(mp, [[(-1., -1.), (3., -1.), (3., 3.), (-1., 3.), (-1., -1.)]]))
WHERE explain LIKE '%Granules:%';

SELECT 'multipolygon far', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_multipolygon
      WHERE polygonsIntersectCartesian(mp, [[(500., 500.), (501., 500.), (501., 501.), (500., 501.), (500., 500.)]]))
WHERE explain LIKE '%Granules:%';

SELECT 'multipolygon rows far', count() FROM test_spatial_bbox_multipolygon
WHERE polygonsIntersectCartesian(mp, [[(500., 500.), (501., 500.), (501., 501.), (500., 501.), (500., 500.)]]);

DROP TABLE test_spatial_bbox_ring;
DROP TABLE test_spatial_bbox_multipolygon;
