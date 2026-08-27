-- Regression test: `pointInPolygon`'s constant-kind rejection used to be checked via the
-- position-blind `rejectsConstGeometryKind`, which fails closed for a `Point` constant at ANY
-- argument position -- including `pointInPolygon`'s own first (point) argument, where `Point` is
-- the legitimate, common case. On top of that,
-- `extractBboxFromFieldValue` treated every `Tuple` constant as opaque, so even a raw, untyped
-- point literal (e.g. `(0.5, 0.5)`, with no explicit `Point` type to trigger kind rejection at
-- all) never contributed a bbox either. Together this meant `WHERE pointInPolygon(const_point,
-- poly)` with `poly` `spatial_bbox`-indexed never pruned any granule, even though a constant point
-- has a well-defined single-coordinate bbox `(x, y, x, y)` that can only match a granule whose
-- polygon bbox could possibly contain that point.

DROP TABLE IF EXISTS test_spatial_bbox_point_in_polygon_const_point_pruning;

CREATE TABLE test_spatial_bbox_point_in_polygon_const_point_pruning
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Granule 1 (ids 1-4): a small polygon whose bbox contains the point (0.5, 0.5) queried below.
INSERT INTO test_spatial_bbox_point_in_polygon_const_point_pruning
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

-- Granule 2 (ids 5-8): a polygon far away -- its bbox cannot contain (0.5, 0.5), must get pruned.
INSERT INTO test_spatial_bbox_point_in_polygon_const_point_pruning
SELECT number + 5, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_point_in_polygon_const_point_pruning FINAL;

-- A raw, untyped point literal as the first argument must prune granule 2.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
        WHERE pointInPolygon((0.5, 0.5), poly)
    )
);

SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
WHERE pointInPolygon((0.5, 0.5), poly);

-- An explicitly Point-typed constant must prune identically.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
        WHERE pointInPolygon(CAST((0.5, 0.5) AS Point), poly)
    )
);

SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
WHERE pointInPolygon(CAST((0.5, 0.5) AS Point), poly);

-- A point outside every granule's bbox must prune everything and return 0.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
        WHERE pointInPolygon((50., 50.), poly)
    )
);

SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
WHERE pointInPolygon((50., 50.), poly);

-- A Point/LineString/MultiPoint/MultiLineString constant in a POLYGON-component argument position
-- must still be rejected -- this fix must not regress the position-aware column rejection
-- confirmed by 04882/04883.
SELECT count() FROM test_spatial_bbox_point_in_polygon_const_point_pruning
WHERE pointInPolygon((0.5, 0.5), CAST((0.4, 0.4) AS Point)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE test_spatial_bbox_point_in_polygon_const_point_pruning;
