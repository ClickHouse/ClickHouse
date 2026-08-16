-- Regression test: `pointInPolygon`'s `rejectsColumnGeometryKind` override used to blanket-return
-- `false` for argument position 0, accepting EVERY named geometry kind there, not just `Point`.
-- `pointInPolygon` only accepts a `Point` (a `Tuple(Float64, Float64)`) in its first argument --
-- `getReturnTypeImpl`'s `validate_tuple(0, ...)` raises `ILLEGAL_TYPE_OF_ARGUMENT` for a `Ring`/
-- `Polygon`/`MultiPolygon`/`LineString`/`MultiPoint`/`MultiLineString` there, since all of those
-- are `Array`s. Trusting such a constant to derive a bbox instead let a query like
-- `WHERE pointInPolygon(readWKT('POLYGON(...)'), poly)` on a `spatial_bbox`-indexed column prune
-- every granule and silently return 0 rather than surfacing the exception -- the same fail-open
-- 04882/04883/04885 fixed for the polygon-component argument positions.

DROP TABLE IF EXISTS test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch;

CREATE TABLE test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- Every granule's bbox is far away from the geometries queried below, so a bbox derived from a
-- first argument that should have been rejected prunes everything and hides the exception.
INSERT INTO test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch FINAL;

-- A `Geometry` (`Variant`) constant carrying a `Polygon` must raise, not prune every granule.
SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(readWKT('POLYGON((500 500,501 500,501 501,500 500))'), poly); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(readWKT('LINESTRING(500 500,501 501)'), poly); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same via a `Dynamic`-typed constant.
SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(CAST([[(500., 500.), (501., 500.), (501., 501.)]], 'Polygon')::Dynamic, poly); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(CAST([(500., 500.), (501., 501.)], 'MultiPoint')::Dynamic, poly); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(CAST([[(500., 500.), (501., 501.)], [(502., 502.), (503., 503.)]], 'MultiLineString')::Dynamic, poly); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- And via a concretely typed constant, with no `Variant`/`Dynamic` wrapper at all.
SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(CAST([(500., 500.), (501., 500.), (501., 501.)], 'Ring'), poly); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A legitimate constant `Point` in the first argument must still prune -- the fix must not regress
-- the pruning 04903 confirms.
SELECT extract(explain_text, '(?s)Name: idx_bbox.*?Granules: ([0-9]+/[0-9]+)') FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS explain_text
    FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
        WHERE pointInPolygon(CAST((500., 500.) AS Point), poly)
    )
);

SELECT count() FROM test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch
WHERE pointInPolygon(CAST((0.5, 0.5) AS Point), poly);

DROP TABLE test_spatial_bbox_point_in_polygon_first_arg_kind_mismatch;
