-- Regression test: `GeoBbox.h`'s `extractBboxFromFieldValue` used to call
-- `boost::geometry::is_valid` on a constant geometry argument unconditionally, disabling `spatial_bbox` pruning for it whenever that check
-- failed -- even with `validate_polygons = 0`, where `pointInPolygon`'s own `executeImpl` skips
-- that same check and never raises for an invalid polygon. Pruning must not fail closed for a
-- predicate that is guaranteed not to throw.

DROP TABLE IF EXISTS test_spatial_bbox_validate_polygons_pruning;

CREATE TABLE test_spatial_bbox_validate_polygons_pruning
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Granule 1 (ids 1-4): points inside the bowtie polygon's bbox [0,0]-[1,1] below.
INSERT INTO test_spatial_bbox_validate_polygons_pruning SELECT number + 1, (0.5, 0.5) FROM numbers(4);

-- Granule 2 (ids 5-8): points far outside that bbox -- must get pruned.
INSERT INTO test_spatial_bbox_validate_polygons_pruning SELECT number + 5, (100., 100.) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_validate_polygons_pruning FINAL;

-- With validate_polygons = 0, the self-intersecting (bowtie) polygon below does not raise, so the
-- spatial_bbox index must still prune granule 2, which lies entirely outside the polygon's bbox.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_validate_polygons_pruning
        WHERE pointInPolygon(p, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
        SETTINGS validate_polygons = 0
    )
);

-- Sanity: with validate_polygons = 0, actual execution must not raise either.
SELECT count() FROM test_spatial_bbox_validate_polygons_pruning
WHERE pointInPolygon(p, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
SETTINGS validate_polygons = 0;

-- Sanity: this fix must not weaken validation when it's actually requested -- the default
-- validate_polygons = 1 must still raise for the same predicate.
SELECT count() FROM test_spatial_bbox_validate_polygons_pruning
WHERE pointInPolygon(p, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)]); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_validate_polygons_pruning;
