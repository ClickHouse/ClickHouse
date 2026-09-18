-- Regression test: sibling spatial conjuncts on the SAME indexed column must be checked against the
-- granule bbox one by one, never against the intersection of their query bboxes.
--
-- The `spatial_bbox` contract is that a matching row's geometry bbox intersects EACH conjunct's
-- query bbox. It is not that it intersects the intersection of them: a single polygon can contain
-- two far-apart points, satisfying both `pointInPolygon` conjuncts, while the two zero-area query
-- bboxes those constants produce have an empty intersection. `extractQueryBbox` used to fold the
-- conjuncts into one box with `max`/`min`, which produced `xmin > xmax` for that query and pruned
-- every granule -- dropping rows that match.

DROP TABLE IF EXISTS test_spatial_bbox_conjunct_not_intersected;

CREATE TABLE test_spatial_bbox_conjunct_not_intersected
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- One big polygon that contains both (0, 0) and (10, 10).
INSERT INTO test_spatial_bbox_conjunct_not_intersected
SELECT number + 1, [[(-1., -1.), (20., -1.), (20., 20.), (-1., 20.)]] FROM numbers(4);

SET optimize_move_to_prewhere = 0;

-- Both conjuncts match every row, so all 4 rows must be returned.
SELECT count() FROM test_spatial_bbox_conjunct_not_intersected
WHERE pointInPolygon((0., 0.), poly) AND pointInPolygon((10., 10.), poly);

-- The granule must be kept: its bbox intersects each query bbox separately.
SELECT 'both conjuncts match', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_conjunct_not_intersected
      WHERE pointInPolygon((0., 0.), poly) AND pointInPolygon((10., 10.), poly))
WHERE explain LIKE '%Granules:%';

-- Pruning must still happen when one conjunct alone cannot match: (500, 500) is outside every
-- granule's bbox, so the granule is dropped regardless of the other conjunct.
SELECT 'one conjunct far away', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_conjunct_not_intersected
      WHERE pointInPolygon((0., 0.), poly) AND pointInPolygon((500., 500.), poly))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_conjunct_not_intersected
WHERE pointInPolygon((0., 0.), poly) AND pointInPolygon((500., 500.), poly);

DROP TABLE test_spatial_bbox_conjunct_not_intersected;
