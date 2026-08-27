-- Regression test: `Ring`/`LineString`/`MultiPoint` all flatten to the same `Array(Point)`-shaped
-- `Field`, and `Polygon`/`MultiLineString` both flatten to the same `Array(Array(Point))`-shaped
-- `Field` -- so `extractBboxFromFieldValue` (src/Common/GeoBbox.h) used to misread a `LineString`,
-- `MultiPoint`, or `MultiLineString` constant, arriving via the `Geometry`/`Variant`-typed
-- `readWKT(...)`, as a `Ring`/`Polygon` and trust a bbox derived from it, even though none of the
-- builtins exercised here (`polygonsIntersectCartesian`, `polygonsWithinCartesian`) accept a
-- `LineString`/`MultiPoint`/`MultiLineString` constant: each unconditionally rejects one with
-- `ILLEGAL_TYPE_OF_ARGUMENT` at evaluation time, using the argument's actual type, not its
-- flattened value. (`pointInPolygon` is the exception among the `isSpatialPredicate()` builtins:
-- it dispatches on `Array` depth alone and runs on those kinds, so pruning stays on for it -- see
-- `05049_spatial_bbox_point_in_polygon_ring_alias_pruning`.) A `Field` alone cannot distinguish these kinds
-- from the ones a predicate does accept, so `extractBboxFromFieldValue` must instead consult the
-- constant's `Geometry` discriminator before flattening, and treat these kinds as opaque to bbox
-- extraction, so pruning is disabled for them and the predicate still runs on real data and raises
-- as expected.

DROP TABLE IF EXISTS test_spatial_bbox_linestring_geometry_kind_mismatch;

CREATE TABLE test_spatial_bbox_linestring_geometry_kind_mismatch
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_linestring_geometry_kind_mismatch
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

INSERT INTO test_spatial_bbox_linestring_geometry_kind_mismatch
SELECT number + 5, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_linestring_geometry_kind_mismatch FINAL;

SELECT toTypeName(readWKT('LINESTRING(500 500, 501 501)'));
SELECT toTypeName(readWKT('MULTIPOINT(500 500, 501 501)'));
SELECT toTypeName(readWKT('MULTILINESTRING((500 500, 501 501),(0.4 0.4, 0.6 0.6))'));

-- Each of these constant geometries lies entirely outside both granules' bboxes: if pruning
-- wrongly trusted a bbox derived from them, every granule would be discarded and the predicate
-- would never run -- must raise instead, not silently prune every granule and return 0.
SELECT count() FROM test_spatial_bbox_linestring_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, readWKT('LINESTRING(500 500, 501 501)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_linestring_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, readWKT('MULTIPOINT(500 500, 501 501)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_linestring_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, readWKT('MULTILINESTRING((500 500, 501 501),(0.4 0.4, 0.6 0.6))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_linestring_geometry_kind_mismatch
WHERE polygonsWithinCartesian(poly, readWKT('LINESTRING(500 500, 501 501)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The legitimate Geometry-typed Polygon constant must still prune and evaluate correctly -- this
-- fix must not regress the working case from 04881.
SELECT count() FROM test_spatial_bbox_linestring_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, readWKT('POLYGON((0.4 0.4,0.6 0.4,0.6 0.6,0.4 0.6,0.4 0.4))'));

DROP TABLE test_spatial_bbox_linestring_geometry_kind_mismatch;
