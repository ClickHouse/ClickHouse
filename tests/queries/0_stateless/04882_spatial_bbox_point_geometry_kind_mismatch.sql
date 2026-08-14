-- Regression test: `extractBboxFromFieldValue` (src/Common/GeoBbox.h) used to treat ANY
-- two-element `Tuple` constant as a `Point` and trust a bbox derived from it, regardless of
-- whether the calling predicate actually accepts a `Point` as its constant geometry argument.
-- None of the builtins that set `isSpatialPredicate()` (`pointInPolygon`,
-- `polygonsIntersectCartesian`, `polygonsWithinCartesian`) do: each unconditionally rejects a
-- `Point` argument with `ILLEGAL_TYPE_OF_ARGUMENT` at evaluation time. A raw `Point` literal is
-- already caught by argument type-checking before ever reaching `GeoBbox.h`, but one that arrives
-- via a `Geometry`/`Variant`-typed constant (e.g. `readWKT('POINT(...)')`) is not -- so pruning
-- would derive a (wrong) bbox from it, discard every granule, and the predicate would never run
-- on real data: the exception it's guaranteed to raise never surfaces, and the query silently
-- returns 0 instead. `extractBboxFromFieldValue` must instead treat every `Tuple`, including a
-- two-element one, as opaque to bbox extraction, so pruning is disabled and the predicate still
-- runs on real data and raises as expected.

DROP TABLE IF EXISTS test_spatial_bbox_point_geometry_kind_mismatch;

CREATE TABLE test_spatial_bbox_point_geometry_kind_mismatch
(
    id   UInt32,
    poly Polygon,
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_point_geometry_kind_mismatch
SELECT number + 1, [[(0.4, 0.4), (0.6, 0.4), (0.6, 0.6), (0.4, 0.6)]] FROM numbers(4);

INSERT INTO test_spatial_bbox_point_geometry_kind_mismatch
SELECT number + 5, [[(100., 100.), (101., 100.), (101., 101.), (100., 101.)]] FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_point_geometry_kind_mismatch FINAL;

SELECT toTypeName(readWKT('POINT(0 0)'));

-- Must raise, not silently prune every granule and return 0.
SELECT count() FROM test_spatial_bbox_point_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, readWKT('POINT(0 0)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_point_geometry_kind_mismatch
WHERE polygonsWithinCartesian(poly, readWKT('POINT(0 0)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Same mismatch with a raw (non-Geometry) Point literal must raise identically, with or without
-- the index -- confirming the fix doesn't change behavior for the already-correct raw-literal path.
SELECT count() FROM test_spatial_bbox_point_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, (0., 0.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- pointInPolygon's legitimate Geometry-typed Polygon constant must still prune and evaluate
-- correctly -- this fix must not regress the working case from 04881.
SELECT count() FROM test_spatial_bbox_point_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(poly, readWKT('POLYGON((0.4 0.4,0.6 0.4,0.6 0.6,0.4 0.6,0.4 0.4))'));

DROP TABLE test_spatial_bbox_point_geometry_kind_mismatch;
