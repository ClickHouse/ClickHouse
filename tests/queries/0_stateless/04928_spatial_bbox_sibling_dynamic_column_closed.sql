-- Regression test: `extractSpatialPredicateNodeBbox` (src/Common/GeoBbox.h) downgraded a spatial
-- predicate whose geometry argument is an `INPUT` that `accept_input` rejects to a plain
-- `has_extra_non_constant`, so the node ended up `NotApplicable` and could not veto pruning for the
-- surrounding `and`. But a `Dynamic`/`Variant` (e.g. `Geometry`) column carries no declared kind at
-- analysis time: whether the predicate accepts it is only decided per row, at execution. With
-- `short_circuit_function_evaluation = 'disable'` such a sibling conjunct is evaluated on every
-- surviving row and is guaranteed to raise once a rejected kind is actually seen -- unless the
-- indexed conjunct pruned every granule first, in which case the adaptors
-- (`ExecutableFunctionDynamicAdaptor`/`ExecutableFunctionVariantAdaptor`) return an empty result
-- without ever building the raising overload, and the query answers `0` instead. That is exception
-- hiding across columns, the same class of hole as the constant-side checks, so a deferred kind
-- rejection on any argument must fail closed for the whole conjunction.

DROP TABLE IF EXISTS test_spatial_bbox_sibling_dynamic;

CREATE TABLE test_spatial_bbox_sibling_dynamic
(
    a Polygon,
    b Dynamic,
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_sibling_dynamic
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]], CAST((100., 100.), 'Point')::Dynamic
FROM numbers(4);

-- `polygonsIntersectCartesian` rejects a `Point` at every argument position, and `b` stores exactly
-- a `Point`, so the query must raise. The `pointInPolygon` conjunct is far from the granule's data
-- bbox, so the buggy pruning path skipped the only granule and answered `0`.
SELECT count() FROM test_spatial_bbox_sibling_dynamic
WHERE polygonsIntersectCartesian(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Same shape through a `Geometry` column (a `Variant` over the geometry kinds), whose per-row kind
-- is likewise unknown until execution.
DROP TABLE IF EXISTS test_spatial_bbox_sibling_geometry;

CREATE TABLE test_spatial_bbox_sibling_geometry
(
    a Polygon,
    b Geometry,
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_sibling_geometry
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]], CAST((100., 100.), 'Point')::Geometry
FROM numbers(4);

SELECT count() FROM test_spatial_bbox_sibling_geometry
WHERE polygonsIntersectCartesian(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sanity: an ordinary (non-`Dynamic`) sibling geometry column that the predicate genuinely accepts
-- must keep pruning working -- failing closed here would cost pruning for no reason.
DROP TABLE IF EXISTS test_spatial_bbox_sibling_polygon;

CREATE TABLE test_spatial_bbox_sibling_polygon
(
    a Polygon,
    b Polygon,
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_sibling_polygon
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]], [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]]
FROM numbers(4);

SELECT count() FROM test_spatial_bbox_sibling_polygon
WHERE polygonsIntersectCartesian(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a)
SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0;

DROP TABLE test_spatial_bbox_sibling_dynamic;
DROP TABLE test_spatial_bbox_sibling_geometry;
DROP TABLE test_spatial_bbox_sibling_polygon;
