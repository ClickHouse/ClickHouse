-- Regression test: an UNNAMED `Dynamic`/`Variant` geometry alternative must not fail `spatial_bbox`
-- pruning closed when the predicate accepts the kind that alternative structurally resolves to.
--
-- `callOnGeometryDataType` resolves a geometry type STRUCTURALLY whenever it carries no custom
-- name: a raw `Tuple(Float64, Float64)` is read as a `Point`, `Array(Tuple(Float64, Float64))` as a
-- `Ring`, and one more `Array` level each as a `Polygon` and a `MultiPolygon`. `geoKindNameOfType`
-- reports no kind for those types, so `hasDeferredGeometryKindRejection` and the constant-side check
-- in `collectConjunctiveSpatialBboxes` used to treat every unnamed alternative as an unknown kind
-- and fail closed, costing pruning for queries that are guaranteed NOT to raise.
--
-- Failing closed is still right for an unnamed alternative whose structural kind the predicate does
-- reject at that argument position -- that overload raises `ILLEGAL_TYPE_OF_ARGUMENT` per row, and
-- pruning the granule away would hide it (see `04928_spatial_bbox_sibling_dynamic_column_closed`).

DROP TABLE IF EXISTS test_spatial_bbox_unnamed_deferred;

CREATE TABLE test_spatial_bbox_unnamed_deferred
(
    a Polygon,
    b Variant(Tuple(Float64, Float64)),
    c Variant(Tuple(Float64, Float64), Array(Array(Tuple(Float64, Float64)))),
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO test_spatial_bbox_unnamed_deferred
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]],
       CAST((100., 100.), 'Tuple(Float64, Float64)'),
       CAST((100., 100.), 'Tuple(Float64, Float64)')
FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- A `Dynamic` constant holding a raw `Tuple(Float64, Float64)` is a `Point` to the predicate, and
-- `pointInPolygon` accepts a `Point` in its first argument, so the query cannot raise on kind
-- grounds and the granule must be pruned by the constant's bbox.
SELECT 'unnamed dynamic point const', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_unnamed_deferred
      WHERE pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)')::Dynamic, a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_unnamed_deferred
WHERE pointInPolygon(CAST((0., 0.), 'Tuple(Float64, Float64)')::Dynamic, a);

-- The same value in the POLYGON argument structurally resolves to a `Point` too, which
-- `pointInPolygon` rejects there, so this must keep failing closed: no pruning, and the query
-- raises.
SELECT 'unnamed dynamic point const rejected', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_unnamed_deferred
      WHERE pointInPolygon((0., 0.), CAST((1., 1.), 'Tuple(Float64, Float64)')::Dynamic)
        AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_unnamed_deferred
WHERE pointInPolygon((0., 0.), CAST((1., 1.), 'Tuple(Float64, Float64)')::Dynamic)
  AND pointInPolygon((0., 0.), a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A sibling conjunct on a `Variant` COLUMN whose only alternative is an unnamed
-- `Tuple(Float64, Float64)`: every alternative resolves to a `Point`, which `pointInPolygon`
-- accepts in its first argument, so no per-row overload can raise and pruning must stay on.
SELECT 'unnamed variant column sibling', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_unnamed_deferred
      WHERE pointInPolygon(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
        AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_unnamed_deferred
WHERE pointInPolygon(b, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])
  AND pointInPolygon((0., 0.), a);

-- An unnamed alternative in the POLYGON argument that the predicate rejects there must keep failing
-- closed. `c` is used rather than `b`: the rejection has to be DEFERRED to per-row dispatch for the
-- pruning decision to exist at all. With `b`'s single, incompatible alternative no overload can be
-- built, so return-type resolution itself raises `ILLEGAL_TYPE_OF_ARGUMENT` while the query is still
-- being analysed -- before any granule is considered. `c` also carries a compatible
-- `Array(Array(Tuple(Float64, Float64)))` (a structural `Polygon`) alternative, so the return type
-- resolves, and only the rows actually holding the `Tuple(Float64, Float64)` alternative -- every row
-- here -- raise at execution time. Pruning the granule away would hide that, so it must stay off.
SELECT 'unnamed variant column sibling rejected', extract(explain, '(Parts:.*|Granules:.*)')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_spatial_bbox_unnamed_deferred
      WHERE pointInPolygon((0., 0.), c) AND pointInPolygon((0., 0.), a))
WHERE explain LIKE '%Granules:%';

SELECT count() FROM test_spatial_bbox_unnamed_deferred
WHERE pointInPolygon((0., 0.), c) AND pointInPolygon((0., 0.), a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE test_spatial_bbox_unnamed_deferred;
