-- Regression test: an argument of `polygonsIntersectCartesian`/`polygonsWithinCartesian` that is no
-- geometry AT ALL -- a number, a `FixedString`, an unrelated column -- must fail `spatial_bbox`
-- pruning closed.
--
-- Both predicates accept the call at build time and only reject such an argument once
-- `callOnGeometryDataType` reads its actual type during `executeImpl`, raising
-- `Unknown geometry type ...`. `extractSpatialPredicateNodeBbox` (`src/Common/GeoBbox.h`) used to
-- see no geometry kind name for those types -- `geoKindNameOfType` and `structuralGeoKindName` both
-- report nothing -- and so treated the argument as "not geometry-shaped", downgrading the conjunct
-- to `NoInfo`/`NotApplicable` instead of failing closed. A sibling conjunct's bbox could then prune
-- every granule away, and the query returned `0` instead of raising.

DROP TABLE IF EXISTS test_spatial_bbox_non_geometry_argument;

CREATE TABLE test_spatial_bbox_non_geometry_argument
(
    poly Polygon,
    s    String,
    f    FixedString(4),
    INDEX idx_bbox_poly poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- Every granule lies far from `(0, 0)`, so the `pointInPolygon` conjunct below prunes all of them.
INSERT INTO test_spatial_bbox_non_geometry_argument
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]], 'abcd', 'abcd' FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- A non-geometry CONSTANT argument: the sibling conjunct must not prune the exception away. The
-- assertion is the exception itself rather than a granule count -- the constant is rejected while
-- the plan's header is computed, so this shape cannot be spelled as a bare `EXPLAIN` (see
-- `05051_spatial_bbox_empty_geometry_no_validate_pruning`).
SELECT count() FROM test_spatial_bbox_non_geometry_argument
WHERE pointInPolygon((0., 0.), poly) AND polygonsIntersectCartesian(poly, 1); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM test_spatial_bbox_non_geometry_argument
WHERE pointInPolygon((0., 0.), poly)
  AND polygonsWithinCartesian(poly, CAST('abcd', 'FixedString(4)')); -- { serverError BAD_ARGUMENTS }

-- A non-geometry sibling COLUMN, which reaches the same `callOnGeometryDataType` rejection.
SELECT count() FROM test_spatial_bbox_non_geometry_argument
WHERE pointInPolygon((0., 0.), poly)
  AND polygonsIntersectCartesian(f, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]]); -- { serverError BAD_ARGUMENTS }

-- A WKB-payload `String` is reported under the kind name `String` and was already failed closed;
-- keep it covered here so the broader non-geometry check does not regress it.
SELECT count() FROM test_spatial_bbox_non_geometry_argument
WHERE pointInPolygon((0., 0.), poly)
  AND polygonsIntersectCartesian(poly, s); -- { serverError BAD_ARGUMENTS }

-- The legitimate all-geometry call must still prune: this fix must not disable pruning wholesale.
SELECT 'all geometry', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_non_geometry_argument
      WHERE pointInPolygon((0., 0.), poly))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_non_geometry_argument
WHERE pointInPolygon((0., 0.), poly);

DROP TABLE test_spatial_bbox_non_geometry_argument;
