-- Regression test: `constGeoKindName` (src/Common/GeoBbox.h) used to only unwrap a constant's
-- `DataTypeVariant` (e.g. `Geometry`) to read the custom name of the concrete alternative stored
-- in it, but not `DataTypeDynamic`. A constant explicitly typed as a geometry kind
-- `polygonsIntersectCartesian`/`polygonsWithinCartesian` are guaranteed to reject (e.g. `Point`,
-- `LineString`, `MultiPoint`, `MultiLineString` -- both only accept `Ring`/`Polygon`/
-- `MultiPolygon`) arriving via a `Dynamic`-typed constant would still fall through to the
-- `getCustomName()` branch for the `Dynamic` type itself (which has none), so
-- `IFunctionBase::rejectsConstGeometryKind` was never consulted, and the constant's flattened
-- `Field` (indistinguishable by shape from an accepted `Ring`/`Polygon`) was trusted to derive a
-- bbox instead -- pruning every granule and silently returning 0 instead of raising
-- `ILLEGAL_TYPE_OF_ARGUMENT`, the same fail-open 04882/04883 already fixed for `Variant`-typed
-- constants like `readWKT(...)`.

DROP TABLE IF EXISTS test_spatial_bbox_dynamic_geometry_kind_mismatch;

CREATE TABLE test_spatial_bbox_dynamic_geometry_kind_mismatch
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_dynamic_geometry_kind_mismatch
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_dynamic_geometry_kind_mismatch FINAL;

SELECT toTypeName(CAST([(500., 500.), (501., 501.)], 'LineString')::Dynamic);

-- Must raise, not silently prune every granule and return 0.
SELECT count() FROM test_spatial_bbox_dynamic_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, CAST((0., 0.), 'Point')::Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_dynamic_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, CAST([(500., 500.), (501., 501.)], 'LineString')::Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_dynamic_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, CAST([(500., 500.), (501., 501.)], 'MultiPoint')::Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_dynamic_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, CAST([[(500., 500.), (501., 501.)], [(502., 502.), (503., 503.)]], 'MultiLineString')::Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_dynamic_geometry_kind_mismatch
WHERE polygonsWithinCartesian(poly, CAST([(500., 500.), (501., 501.)], 'LineString')::Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A legitimate Dynamic-typed Polygon constant must still prune and evaluate correctly -- this fix
-- must not regress the working case.
SELECT count() FROM test_spatial_bbox_dynamic_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, CAST([[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]], 'Polygon')::Dynamic);

DROP TABLE test_spatial_bbox_dynamic_geometry_kind_mismatch;
