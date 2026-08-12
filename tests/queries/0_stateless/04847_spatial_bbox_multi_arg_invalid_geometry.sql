-- Regression test: `MergeTreeIndexConditionSpatialBbox::extractQueryBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- validated each constant geometry argument of a multi-argument `pointInPolygon(point, arg1, arg2, ...)`
-- INDEPENDENTLY, then unioned their individual bboxes. `FunctionPointInPolygon` instead assembles all
-- constant arguments into ONE combined geometry (a `Polygon` with holes, or a `MultiPolygon`) and validates
-- THAT with `bg::is_valid`, matching `pointInPolygon.cpp`'s `is_const_multi_polygon` logic. A hole that lies
-- entirely outside its shell is invalid only once assembled -- each ring is a perfectly valid ring on its
-- own -- so the old per-child validation missed it, producing a small bogus bbox from the union of the
-- (individually valid) shell and hole. Rows far outside that bogus bbox got pruned and skipped entirely,
-- silently hiding the "Polygon is not valid" exception that `pointInPolygon` raises at execute time for
-- every row, instead of failing closed (no pruning) so the exception is always raised.

DROP TABLE IF EXISTS test_spatial_bbox_multi_arg_invalid_geometry;

CREATE TABLE test_spatial_bbox_multi_arg_invalid_geometry
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

-- All rows lie far outside the small shell+hole bbox near the origin, so a bbox extractor that unions
-- the two (individually valid) rings' bboxes independently would prune every granule and hide the
-- exception that `pointInPolygon` must raise because the hole lies entirely outside the shell.
INSERT INTO test_spatial_bbox_multi_arg_invalid_geometry SELECT number + 1, (1000. + number, 1000. + number) FROM numbers(8);

SELECT count() FROM test_spatial_bbox_multi_arg_invalid_geometry
WHERE pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)], [(50., 50.), (51., 50.), (51., 51.), (50., 51.)]); -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_multi_arg_invalid_geometry;
