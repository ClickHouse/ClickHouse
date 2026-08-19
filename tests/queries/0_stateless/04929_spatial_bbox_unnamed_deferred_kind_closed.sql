-- Regression test: two remaining fail-open holes around geometry kinds that are only resolved at
-- execution time, both reached from a sibling `and` conjunct while the indexed conjunct prunes.
--
-- `callOnGeometryDataType` resolves a geometry STRUCTURALLY, so a raw `Tuple(Float64, Float64)`
-- is read as a `Point` even though it carries no custom kind name. `hasDeferredGeometryKindRejection`
-- treated an unnamed `Variant` alternative as saying nothing, and the constant branch of
-- `extractSpatialPredicateNodeBbox` did the same for a `Dynamic`/`Variant` constant whose stored
-- alternative is unnamed: `tryExtractConstGeoField` flattens it to a raw tuple that
-- `extractBboxFromFieldValue` declines without poisoning `acc.valid`. Either way the node was
-- downgraded to `NotApplicable`, the indexed conjunct pruned the only granule, and the per-row
-- overload that raises `ILLEGAL_TYPE_OF_ARGUMENT` was never built -- a silent `0`. An unnamed
-- alternative is an unknown kind, not an absent one, and must fail closed. See
-- 04928_spatial_bbox_sibling_dynamic_column_closed for the named-kind half of the same hole.

DROP TABLE IF EXISTS test_spatial_bbox_unnamed_variant_column;

CREATE TABLE test_spatial_bbox_unnamed_variant_column
(
    a Polygon,
    b Variant(Tuple(Float64, Float64)),
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_unnamed_variant_column
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]],
       CAST((100., 100.) AS Tuple(Float64, Float64))::Variant(Tuple(Float64, Float64))
FROM numbers(4);

SELECT count() FROM test_spatial_bbox_unnamed_variant_column
WHERE polygonsIntersectCartesian(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS test_spatial_bbox_unnamed_deferred_const;

CREATE TABLE test_spatial_bbox_unnamed_deferred_const
(
    a Polygon,
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_unnamed_deferred_const
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]] FROM numbers(4);

-- A `Dynamic` constant holding an unnamed `Tuple(Float64, Float64)`, in a conjunct that doesn't even
-- reference the indexed column -- it is still evaluated on every surviving row and still raises.
SELECT count() FROM test_spatial_bbox_unnamed_deferred_const
WHERE polygonsIntersectCartesian([[(1., 1.), (2., 1.), (2., 2.), (1., 1.)]], CAST((500., 500.) AS Tuple(Float64, Float64))::Dynamic)
  AND pointInPolygon((0., 0.), a)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: a `Variant` all of whose alternatives are kinds the predicate accepts must still prune --
-- failing closed for it would cost pruning for nothing.
DROP TABLE IF EXISTS test_spatial_bbox_accepted_variant_column;

CREATE TABLE test_spatial_bbox_accepted_variant_column
(
    a Polygon,
    b Variant(Polygon, MultiPolygon),
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_accepted_variant_column
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]],
       CAST([[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]] AS Polygon)::Variant(Polygon, MultiPolygon)
FROM numbers(4);

SELECT count() FROM test_spatial_bbox_accepted_variant_column
WHERE polygonsIntersectCartesian(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0;

DROP TABLE test_spatial_bbox_unnamed_variant_column;
DROP TABLE test_spatial_bbox_unnamed_deferred_const;
DROP TABLE test_spatial_bbox_accepted_variant_column;
