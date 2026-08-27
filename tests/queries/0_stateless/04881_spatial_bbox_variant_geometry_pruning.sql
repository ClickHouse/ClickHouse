-- Regression test: a constant geometry argument passed as the generic `Geometry` type (a
-- custom-named `Variant`) is dispatched via `FunctionBaseVariantAdaptor`, which used to hardcode
-- `requiresValidConstGeometry() = true` regardless of what the underlying resolved function
-- actually does. This made `spatial_bbox`
-- pruning fail closed for a `Geometry`-typed constant argument even when the same argument passed
-- as a raw array literal would prune correctly (e.g. `pointInPolygon` with `validate_polygons = 0`,
-- or `polygonsIntersectCartesian`/`polygonsWithinCartesian`, which never validate topology at all).

DROP TABLE IF EXISTS test_spatial_bbox_variant_geometry_pruning;

CREATE TABLE test_spatial_bbox_variant_geometry_pruning
(
    id UInt32,
    p    Point,
    poly Polygon,
    INDEX idx_bbox_p    p    TYPE spatial_bbox GRANULARITY 1,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Granule 1 (ids 1-4): inside the bowtie polygon's bbox [0,0]-[1,1] below.
INSERT INTO test_spatial_bbox_variant_geometry_pruning
SELECT number + 1, (0.5, 0.5), [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

-- Granule 2 (ids 5-8): far outside that bbox -- must get pruned.
INSERT INTO test_spatial_bbox_variant_geometry_pruning
SELECT number + 5, (100., 100.), [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_variant_geometry_pruning FINAL;

SELECT toTypeName(readWKT('POLYGON((0 0,1 1,1 0,0 1,0 0))'));

-- pointInPolygon with the self-intersecting bowtie polygon passed as Geometry (readWKT), under
-- validate_polygons = 0: must still prune granule 2 and must not raise.
SELECT extract(explain_text, '(?s)Name: idx_bbox_p.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_variant_geometry_pruning
        WHERE pointInPolygon(p, readWKT('POLYGON((0 0,1 1,1 0,0 1,0 0))'))
        SETTINGS validate_polygons = 0
    )
);

SELECT count() FROM test_spatial_bbox_variant_geometry_pruning
WHERE pointInPolygon(p, readWKT('POLYGON((0 0,1 1,1 0,0 1,0 0))'))
SETTINGS validate_polygons = 0;

-- polygonsIntersectCartesian with the same bowtie polygon passed as Geometry: never validates
-- topology, so pruning must stay active and evaluation must not raise, with no special setting.
SELECT extract(explain_text, '(?s)Name: idx_bbox_poly.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_variant_geometry_pruning
        WHERE polygonsIntersectCartesian(poly, readWKT('POLYGON((0 0,1 1,1 0,0 1,0 0))'))
    )
);

SELECT count() FROM test_spatial_bbox_variant_geometry_pruning
WHERE polygonsIntersectCartesian(poly, readWKT('POLYGON((0 0,1 1,1 0,0 1,0 0))'));

DROP TABLE test_spatial_bbox_variant_geometry_pruning;
