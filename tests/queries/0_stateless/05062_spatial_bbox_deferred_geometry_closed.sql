-- Regression test: an argument whose geometry kind is settled only per row -- a `Geometry` (a
-- `Variant`) or a `Dynamic`, constant or column -- must fail `spatial_bbox` pruning CLOSED,
-- unconditionally.
--
-- A spatial predicate validates its arguments in `getReturnTypeImpl`, during analysis, before any
-- granule is read. That guarantee does not hold for these types: `FunctionVariantAdaptor` /
-- `FunctionDynamicAdaptor` build the concrete overload -- and therefore run `getReturnTypeImpl` --
-- only while EXECUTING, per row, and nothing evaluates the predicate before granule selection,
-- because `ActionsDAG::updateHeader` dry-runs a function only when every argument is constant. If a
-- sibling conjunct's bbox pruned every granule away first,
-- `ExecutableFunctionVariantAdaptor`/`ExecutableFunctionDynamicAdaptor` return an empty result
-- without ever building the raising overload, and the query answers `0` instead of raising
-- `ILLEGAL_TYPE_OF_ARGUMENT`.
--
-- The veto is deliberately blunt: `GeoBboxDetail::isDeferredGeometryKindType` (`Common/GeoBbox.h`)
-- consults neither the predicate nor the kind actually stored, so pruning is given up even where the
-- overload would have run fine (the `Point` cases below). Resolving the stored kind and asking the
-- predicate about it needs per-kind knowledge in every spatial predicate; that refinement is
-- https://github.com/ClickHouse/ClickHouse/issues/117163.

DROP TABLE IF EXISTS test_spatial_bbox_deferred;

SET optimize_move_to_prewhere = 0;

CREATE TABLE test_spatial_bbox_deferred
(
    id UInt32,
    g  Polygon,
    v  Geometry,
    w  Polygon,
    INDEX idx_bbox_g g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

-- The single granule sits around (100, 100), far from every constant below, so the index would drop
-- it if pruning were allowed to proceed.
INSERT INTO test_spatial_bbox_deferred
VALUES (1,
        [[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]],
        readWKT('POINT(1 1)'),
        [[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]]);

-- A `Geometry` CONSTANT holding a kind the predicate refuses: the granule must survive, and the
-- query must raise rather than answer `0`.
SELECT 'geometry polygon const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_deferred
      WHERE pointInPolygon(CAST(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS Geometry), g))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_deferred
WHERE pointInPolygon(CAST(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS Geometry), g); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A `Geometry` constant holding a `Point` runs fine, but pruning is given up for it all the same.
SELECT 'geometry point const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_deferred
      WHERE pointInPolygon(CAST(readWKTPoint('POINT(0 0)') AS Geometry), g))
WHERE explain LIKE '%Granules:%';

SELECT 'geometry point const', count() FROM test_spatial_bbox_deferred
WHERE pointInPolygon(CAST(readWKTPoint('POINT(0 0)') AS Geometry), g);

-- A `Geometry` COLUMN in a sibling conjunct vetoes pruning on the indexed `g` column too. A lenient
-- session must fail closed just like a strict one: `ExecutableFunctionVariantAdaptor::try_execute`
-- re-throws an `ILLEGAL_TYPE_OF_ARGUMENT` raised during EXECUTION, and
-- `polygonsIntersectCartesian` refuses `Point` only in `executeImpl`, so the exception survives
-- `variant_throw_on_type_mismatch = 0`.
SET variant_throw_on_type_mismatch = 0;

SELECT 'lenient geometry sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_deferred
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]])
        AND polygonsIntersectCartesian(v, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

SET variant_throw_on_type_mismatch = 1;

SELECT 'strict geometry sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_deferred
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]])
        AND polygonsIntersectCartesian(v, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

-- Control: a sibling conjunct on a plainly typed column leaves pruning on, so every veto above is a
-- real decision rather than the index never being applicable to this query shape.
SELECT 'accepted sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_deferred
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]])
        AND polygonsIntersectCartesian(w, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

DROP TABLE test_spatial_bbox_deferred;
