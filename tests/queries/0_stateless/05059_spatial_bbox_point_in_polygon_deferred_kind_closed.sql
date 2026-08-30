-- Regression test: `pointInPolygon` must fail `spatial_bbox` pruning CLOSED for a `Geometry` or
-- `Dynamic` constant at its first argument that does not hold a `Point`.
--
-- `pointInPolygon` validates its arguments structurally in `getReturnTypeImpl`, so a plainly typed
-- polygon at argument 0 is refused during analysis, before any granule is read. That guarantee does
-- NOT hold for a `Geometry` (a `Variant`) or a `Dynamic` constant: `FunctionVariantAdaptor` /
-- `FunctionDynamicAdaptor` build the concrete overload -- and therefore run `getReturnTypeImpl` --
-- only while EXECUTING, per row. Nothing evaluates the predicate before granule selection, because
-- `ActionsDAG::updateHeader` dry-runs a function only when every argument is constant, and the
-- polygon argument here is a column. So the bbox of that constant must not be used to prune: with
-- the granule dropped the overload is never built and the query answers `0` instead of raising
-- `ILLEGAL_TYPE_OF_ARGUMENT`.
--
-- `pointInPolygon` states this through `rejectsColumnGeometryKind`, the same surviving hook
-- `polygonsIntersectCartesian` and `polygonsWithinCartesian` use.

DROP TABLE IF EXISTS test_spatial_bbox_pip_deferred;

CREATE TABLE test_spatial_bbox_pip_deferred
(
    g Polygon,
    INDEX idx_bbox_g g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

-- The only granule sits near (100, 100), far from every constant below.
INSERT INTO test_spatial_bbox_pip_deferred VALUES ([[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]]);

SET optimize_move_to_prewhere = 0;

-- A `Geometry` constant holding a `Polygon` at argument 0: the granule must survive, and the query
-- must raise rather than answer `0`.
SELECT 'geometry polygon const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_pip_deferred
      WHERE pointInPolygon(CAST(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS Geometry), g))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_pip_deferred
WHERE pointInPolygon(CAST(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS Geometry), g); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A `Point` constant wrapped in `Geometry` is accepted by the overload built per row, so pruning
-- must stay ON for it -- failing closed everywhere would cost pruning for a query that cannot raise.
SELECT 'geometry point const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_pip_deferred
      WHERE pointInPolygon(CAST(readWKTPoint('POINT(0 0)') AS Geometry), g))
WHERE explain LIKE '%Granules:%';

SELECT 'geometry point const', count() FROM test_spatial_bbox_pip_deferred
WHERE pointInPolygon(CAST(readWKTPoint('POINT(0 0)') AS Geometry), g);

DROP TABLE test_spatial_bbox_pip_deferred;
