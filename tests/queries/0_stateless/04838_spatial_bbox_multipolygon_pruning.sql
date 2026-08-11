-- Regression test: pointInPolygon is variadic and accepts a MultiPolygon as several
-- constant polygon arguments, e.g. pointInPolygon(geom, poly1, poly2, ...).
-- The spatial_bbox skip index condition must derive the query bbox as the union of
-- ALL constant polygon arguments, not just the first one, or it will incorrectly
-- prune granules that only match a later polygon argument.

DROP TABLE IF EXISTS test_spatial_bbox_multipolygon_pruning;

CREATE TABLE test_spatial_bbox_multipolygon_pruning
(
    id   UInt32,
    geom Point,
    INDEX idx_bbox geom TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- ids 1-8:  points near (1,1)-(4,4)
-- ids 9-16: points near (20,20)-(23,23)
INSERT INTO test_spatial_bbox_multipolygon_pruning SELECT
    number + 1 AS id,
    (toFloat64((number % 4) + 1), toFloat64((number % 4) + 1)) AS geom
FROM numbers(8);

INSERT INTO test_spatial_bbox_multipolygon_pruning SELECT
    number + 9 AS id,
    (toFloat64((number % 4) + 20), toFloat64((number % 4) + 20)) AS geom
FROM numbers(8);

-- Merge into one part so skip index granule pruning is exercised.
OPTIMIZE TABLE test_spatial_bbox_multipolygon_pruning FINAL;

-- The first polygon argument covers nothing in the table (far away, near (50,50)).
-- The second polygon argument covers only ids 9-16, near (20,20)-(23,23).
-- A query bbox derived from only the first argument would be disjoint from every
-- granule and incorrectly prune all of them, wrongly returning no rows.
SELECT id FROM test_spatial_bbox_multipolygon_pruning
WHERE pointInPolygon(
    geom,
    [[(50., 50.), (55., 50.), (55., 55.), (50., 55.), (50., 50.)]],
    [[(18., 18.), (25., 18.), (25., 25.), (18., 25.), (18., 18.)]])
ORDER BY id;

DROP TABLE test_spatial_bbox_multipolygon_pruning;
