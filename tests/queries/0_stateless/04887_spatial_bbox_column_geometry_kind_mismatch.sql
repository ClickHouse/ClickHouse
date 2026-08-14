-- Regression test: `extractSpatialPredicateNodeBbox` (src/Common/GeoBbox.h) used to accept ANY
-- `INPUT` node (a plain column reference) as the bbox-contributing operand of a spatial predicate,
-- regardless of the column's own geometry kind. `IFunctionBase::rejectsConstGeometryKind` was only
-- ever consulted for CONSTANT arguments. A concretely-typed (non-`Variant`/`Dynamic`) column of a
-- kind `polygonsIntersectCartesian`/`polygonsWithinCartesian` is guaranteed to reject at every
-- argument position (e.g. `Point`) could still have its indexed `spatial_bbox` statistics trusted
-- to derive a pruning bbox from the OTHER (constant) argument, silently pruning away every granule
-- instead of raising `ILLEGAL_TYPE_OF_ARGUMENT` once the predicate is actually evaluated.

DROP TABLE IF EXISTS test_spatial_bbox_column_geometry_kind_mismatch;

CREATE TABLE test_spatial_bbox_column_geometry_kind_mismatch
(
    id UInt32,
    p  Point,
    INDEX idx_bbox_p p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_column_geometry_kind_mismatch
SELECT number + 1, (500. + number, 500. + number) FROM numbers(4);

OPTIMIZE TABLE test_spatial_bbox_column_geometry_kind_mismatch FINAL;

-- `polygonsIntersectCartesian` rejects a `Point` argument at any position. The indexed column `p`
-- is genuinely `Point`-typed, and the query polygon is far from the granule's data bbox -- the
-- buggy pruning path would derive a bbox from the constant query polygon and skip the only
-- granule, silently returning 0 instead of raising.
SELECT count() FROM test_spatial_bbox_column_geometry_kind_mismatch
WHERE polygonsIntersectCartesian(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM test_spatial_bbox_column_geometry_kind_mismatch
WHERE polygonsWithinCartesian(p, [[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: `pointInPolygon`'s position-0 exemption (its first argument legitimately accepts a
-- `Point`-typed column) must not be regressed -- pruning and evaluation must still work.
SELECT count() FROM test_spatial_bbox_column_geometry_kind_mismatch
WHERE pointInPolygon(p, [(499., 499.), (505., 499.), (505., 505.), (499., 505.)]);

DROP TABLE test_spatial_bbox_column_geometry_kind_mismatch;
