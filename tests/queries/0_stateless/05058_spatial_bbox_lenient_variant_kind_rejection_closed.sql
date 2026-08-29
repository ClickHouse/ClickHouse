-- Regression test: the lenient `Variant`/`Dynamic` adaptors must not mask a geometry kind rejection
-- from `spatial_bbox` pruning.
--
-- `FunctionBaseVariantAdaptor`/`FunctionBaseDynamicAdaptor` used to answer `rejectsColumnGeometryKind`
-- with `false` whenever `variant_throw_on_type_mismatch`/`dynamic_throw_on_type_mismatch` was off and
-- the rejected kind sat on the dispatched argument, on the reasoning that the adaptor swallows the
-- rejection and resolves those rows to `NULL`, leaving no exception for pruning to hide.
--
-- That reasoning holds only for a rejection raised while BUILDING the wrapped function.
-- `ExecutableFunctionVariantAdaptor::try_execute` deliberately re-throws an `ILLEGAL_TYPE_OF_ARGUMENT`
-- raised during execution, and `polygonsIntersectCartesian`/`polygonsWithinCartesian` reject `Point`,
-- `LineString`, `MultiLineString` and `MultiPoint` only in `executeImpl`. Under the lenient setting
-- such a predicate therefore still raises, so pruning a sibling conjunct's granule away could hide it.
--
-- `Geometry` is a `Variant` whose alternatives include `Point`, so `hasDeferredGeometryKindRejection`
-- must veto pruning for the whole conjunction regardless of the setting.

DROP TABLE IF EXISTS test_spatial_bbox_lenient_variant_closed;

SET optimize_move_to_prewhere = 0;

CREATE TABLE test_spatial_bbox_lenient_variant_closed
(
    id UInt32,
    g  Polygon,
    v  Geometry,
    w  Polygon,
    INDEX idx_bbox_g g TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

-- The single granule sits around (100, 100), far from the (0, 0)-(10, 10) constant every query below
-- passes, so the index would drop it if pruning were allowed to proceed.
INSERT INTO test_spatial_bbox_lenient_variant_closed
VALUES (1,
        [[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]],
        readWKT('POINT(1 1)'),
        [[(100., 100.), (110., 100.), (110., 110.), (100., 110.), (100., 100.)]]);

-- A lenient session must fail closed just like a strict one: `Granules: 1/1`, no skip-index entry.
SET variant_throw_on_type_mismatch = 0;

SELECT 'lenient geometry sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_variant_closed
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]])
        AND polygonsIntersectCartesian(v, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

SET variant_throw_on_type_mismatch = 1;

SELECT 'strict geometry sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_variant_closed
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]])
        AND polygonsIntersectCartesian(v, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

-- Control: a sibling conjunct on a kind the predicate accepts leaves pruning on, so the veto above
-- is a real decision rather than the index never being applicable to this query shape.
SELECT 'accepted sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_lenient_variant_closed
      WHERE polygonsIntersectCartesian(g, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]])
        AND polygonsIntersectCartesian(w, [[(0., 0.), (10., 0.), (10., 10.), (0., 10.), (0., 0.)]]))
WHERE explain LIKE '%Granules:%';

DROP TABLE test_spatial_bbox_lenient_variant_closed;
