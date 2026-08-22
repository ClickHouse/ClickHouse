-- Regression test: `hasDeferredGeometryKindRejection` (`src/Common/GeoBbox.h`) fails `spatial_bbox`
-- pruning closed whenever a sibling `Variant`/`Dynamic` geometry argument could resolve, per row, to
-- a kind the predicate rejects, because pruning the granule away would hide the
-- `ILLEGAL_TYPE_OF_ARGUMENT` that evaluating the sibling raises (see
-- `04928_spatial_bbox_sibling_dynamic_column_closed`).
--
-- That reasoning holds only while the function actually raises. With
-- `variant_throw_on_type_mismatch = 0` / `dynamic_throw_on_type_mismatch = 0`,
-- `ExecutableFunctionVariantAdaptor` / `ExecutableFunctionDynamicAdaptor` swallow the build-time
-- `ILLEGAL_TYPE_OF_ARGUMENT` of an incompatible alternative and resolve those rows to NULL instead,
-- so there is no exception left to hide and pruning must stay on.
-- `FunctionBaseVariantAdaptor` / `FunctionBaseDynamicAdaptor` forwarded `rejectsColumnGeometryKind`
-- unconditionally, which cost pruning in such sessions for nothing.
--
-- `pointInPolygon` is used as the sibling on purpose: it is dispatched through those adaptors, so it
-- is the case leniency actually applies to. A predicate that handles `Variant` itself (e.g.
-- `polygonsIntersectCartesian`) keeps raising regardless of the setting and must keep failing closed.

DROP TABLE IF EXISTS test_spatial_bbox_lenient_geometry;

CREATE TABLE test_spatial_bbox_lenient_geometry
(
    a Polygon,
    b Geometry,
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_lenient_geometry
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]], CAST((100., 100.), 'Point')::Geometry
FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- Strict (the default): the sibling is guaranteed to raise, so pruning must stay off and the query
-- must raise rather than answer from a granule that was pruned away.
SELECT count() FROM test_spatial_bbox_lenient_geometry
WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'strict variant', trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_geometry
      WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SET variant_throw_on_type_mismatch = 0;

-- Lenient: the sibling resolves to NULL instead of raising, so the granule must be pruned.
SELECT 'lenient variant', trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_geometry
      WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_lenient_geometry
WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a);

-- A predicate that handles `Variant` itself still raises under the lenient setting, so it must keep
-- failing closed.
SELECT count() FROM test_spatial_bbox_lenient_geometry
WHERE polygonsIntersectCartesian(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS test_spatial_bbox_lenient_dynamic;

CREATE TABLE test_spatial_bbox_lenient_dynamic
(
    a Polygon,
    b Dynamic,
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_lenient_dynamic
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]], CAST((100., 100.), 'Point')::Dynamic
FROM numbers(4);

SELECT 'strict dynamic', trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_dynamic
      WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SET dynamic_throw_on_type_mismatch = 0;

SELECT 'lenient dynamic', trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_dynamic
      WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_lenient_dynamic
WHERE pointInPolygon((0., 0.), b) AND pointInPolygon((0., 0.), a);

DROP TABLE test_spatial_bbox_lenient_geometry;
DROP TABLE test_spatial_bbox_lenient_dynamic;
