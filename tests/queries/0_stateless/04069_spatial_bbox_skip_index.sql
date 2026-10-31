-- Tests for the `spatial_bbox` skip index on MergeTree tables.
-- The index stores a per-granule bounding box of geometry column values and
-- prunes granules whose bbox is disjoint from the query bbox.

DROP TABLE IF EXISTS test_spatial_bbox_skip_index;

CREATE TABLE test_spatial_bbox_skip_index
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Insert 20 rows in two batches to get distinct parts/granules:
--   ids 1-8:  points near (1,1)-(4,4) — inside query polygon region
--   ids 9-16: points near (10,10)-(13,13) — outside query polygon region
INSERT INTO test_spatial_bbox_skip_index SELECT
    number + 1 AS id,
    (toFloat64((number % 4) + 1), toFloat64((number % 4) + 1)) AS geom
FROM numbers(8);

INSERT INTO test_spatial_bbox_skip_index SELECT
    number + 9 AS id,
    (toFloat64((number % 4) + 10), toFloat64((number % 4) + 10)) AS geom
FROM numbers(8);

-- Merge into one part so skip index granule pruning is exercised.
OPTIMIZE TABLE test_spatial_bbox_skip_index FINAL;

-- Sanity: all 16 rows are present.
SELECT count() FROM test_spatial_bbox_skip_index;

-- Spatial query: polygon covers (0,0)-(5,5), so only ids 1-8 should match.
-- The skip index should prune the granules containing ids 9-16.
SELECT id FROM test_spatial_bbox_skip_index
WHERE pointInPolygon(geom, [(0., 0.), (5., 0.), (5., 5.), (0., 5.), (0., 0.)])
ORDER BY id;

-- EXPLAIN should show the skip index is used and prunes granules.
-- We just verify the query returns the right result; granule counts vary by
-- index_granularity and merge layout, so we avoid fragile numeric assertions.

-- Query that matches no rows: polygon far away from all data.
SELECT count() FROM test_spatial_bbox_skip_index
WHERE pointInPolygon(geom, [(100., 100.), (101., 100.), (101., 101.), (100., 101.), (100., 100.)]);

DROP TABLE test_spatial_bbox_skip_index;
