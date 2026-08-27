-- Positive `spatial_bbox` pruning coverage for the indexed-column shapes that
-- `registerStorageMergeTree.cpp` documents as supported but the rest of the suite never creates an
-- index on: `Ring` (`Array(Tuple(Float64, Float64))`) and `MultiPolygon`
-- (`Array(Array(Array(Tuple(Float64, Float64))))`).
--
-- The aggregator walks the nested `ColumnArray` levels down to the innermost
-- `Tuple(Float64, Float64)`, so each shape exercises a different depth of that walk (`Point` and
-- `Polygon` are covered elsewhere). Each shape is checked both with a query bbox that must keep a
-- granule and one that must prune both, so a regression that stopped deriving granule bboxes at one
-- of these depths cannot pass by pruning everything away.

DROP TABLE IF EXISTS test_spatial_bbox_ring;
DROP TABLE IF EXISTS test_spatial_bbox_multipolygon;

SET optimize_move_to_prewhere = 0;

CREATE TABLE test_spatial_bbox_ring
(
    r Ring,
    INDEX idx_bbox_r r TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- One granule near the origin, one far away, in separate parts.
INSERT INTO test_spatial_bbox_ring VALUES ([(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]);
INSERT INTO test_spatial_bbox_ring VALUES ([(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]);

SELECT 'ring', count() FROM test_spatial_bbox_ring
WHERE polygonsIntersectCartesian(r, [[(0., 0.), (20., 0.), (20., 20.), (0., 20.), (0., 0.)]]);

SELECT 'ring', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_ring
      WHERE polygonsIntersectCartesian(r, [[(0., 0.), (20., 0.), (20., 20.), (0., 20.), (0., 0.)]]))
WHERE explain LIKE '%Granules: %/%';

SELECT 'ring far', count() FROM test_spatial_bbox_ring
WHERE polygonsIntersectCartesian(r, [[(1000., 1000.), (1010., 1000.), (1010., 1010.), (1000., 1010.), (1000., 1000.)]]);

SELECT 'ring far', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_ring
      WHERE polygonsIntersectCartesian(r, [[(1000., 1000.), (1010., 1000.), (1010., 1010.), (1000., 1010.), (1000., 1000.)]]))
WHERE explain LIKE '%Granules: %/%';

CREATE TABLE test_spatial_bbox_multipolygon
(
    m MultiPolygon,
    INDEX idx_bbox_m m TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_multipolygon VALUES ([[[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]]);
INSERT INTO test_spatial_bbox_multipolygon VALUES ([[[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]]]);

SELECT 'multipolygon', count() FROM test_spatial_bbox_multipolygon
WHERE polygonsIntersectCartesian(m, [[(0., 0.), (20., 0.), (20., 20.), (0., 20.), (0., 0.)]]);

SELECT 'multipolygon', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_multipolygon
      WHERE polygonsIntersectCartesian(m, [[(0., 0.), (20., 0.), (20., 20.), (0., 20.), (0., 0.)]]))
WHERE explain LIKE '%Granules: %/%';

SELECT 'multipolygon far', count() FROM test_spatial_bbox_multipolygon
WHERE polygonsIntersectCartesian(m, [[(1000., 1000.), (1010., 1000.), (1010., 1010.), (1000., 1010.), (1000., 1000.)]]);

SELECT 'multipolygon far', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_multipolygon
      WHERE polygonsIntersectCartesian(m, [[(1000., 1000.), (1010., 1000.), (1010., 1010.), (1000., 1010.), (1000., 1000.)]]))
WHERE explain LIKE '%Granules: %/%';

DROP TABLE test_spatial_bbox_ring;
DROP TABLE test_spatial_bbox_multipolygon;
