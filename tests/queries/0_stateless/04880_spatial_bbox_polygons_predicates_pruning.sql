-- Regression test: `polygonsIntersectCartesian` and `polygonsWithinCartesian` never call
-- `boost::geometry::is_valid` in their `executeImpl` (only `boost::geometry::correct`), so they
-- never raise for an invalid (e.g. self-intersecting) constant polygon argument, unlike
-- `pointInPolygon`. Before `requiresValidConstGeometry()` was overridden to `false` for them,
-- `spatial_bbox` pruning was disabled for such a constant argument even though evaluating the
-- predicate is guaranteed not to throw.

DROP TABLE IF EXISTS test_spatial_bbox_polygons_predicates_pruning;

CREATE TABLE test_spatial_bbox_polygons_predicates_pruning
(
    id UInt32,
    poly Polygon,
    INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Granule 1 (ids 1-4): a small polygon inside the bowtie polygon's bbox [0,0]-[1,1] below.
INSERT INTO test_spatial_bbox_polygons_predicates_pruning
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

-- Granule 2 (ids 5-8): a polygon far outside that bbox -- must get pruned.
INSERT INTO test_spatial_bbox_polygons_predicates_pruning
SELECT number + 5, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_polygons_predicates_pruning FINAL;

-- The bowtie polygon below is self-intersecting, but polygonsIntersectCartesian never validates
-- topology, so it never raises -- the spatial_bbox index must still prune granule 2.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_polygons_predicates_pruning
        WHERE polygonsIntersectCartesian(poly, [[(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]])
    )
);

-- Sanity: execution must not raise either.
SELECT count() FROM test_spatial_bbox_polygons_predicates_pruning
WHERE polygonsIntersectCartesian(poly, [[(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]]);

-- Same, for polygonsWithinCartesian.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_polygons_predicates_pruning
        WHERE polygonsWithinCartesian(poly, [[(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]])
    )
);

SELECT count() FROM test_spatial_bbox_polygons_predicates_pruning
WHERE polygonsWithinCartesian(poly, [[(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]]);

DROP TABLE test_spatial_bbox_polygons_predicates_pruning;
