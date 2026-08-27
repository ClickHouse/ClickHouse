-- Regression test: a spatial predicate that resolves to `Nullable(Nothing)` must not fail
-- `spatial_bbox` pruning closed.
--
-- That is the strongest lenient state of `variant_throw_on_type_mismatch = 0`: when EVERY
-- alternative of the `Variant` argument is incompatible, `FunctionBaseVariantAdaptor` resolves the
-- whole node to `Nullable(Nothing)` and `ExecutableFunctionVariantAdaptor` returns NULL rows
-- without ever building the wrapped predicate. `extractSpatialPredicateNodeBbox`
-- (`src/Common/GeoBbox.h`) still inspected the arguments and failed closed on the WKB `String` in
-- the polygon position -- a rejection that is dead code here, since
-- `FunctionPointInPolygon::getReturnTypeImpl` raises on argument 0 long before it inspects argument
-- 1 -- so the sibling conjunct on the indexed column lost its pruning.
--
-- The pruning itself is not observable through the result: a conjunct that is NULL on every row
-- makes the whole `WHERE` select nothing, so the query answers `0` either way and the plan may drop
-- the read altogether. What the assertions below pin down is the pair of behaviours that ARE
-- observable, and that the fix must not disturb: no exception under the lenient setting, and the
-- exception still surfacing under the default one.

DROP TABLE IF EXISTS test_spatial_bbox_nothing_result;

CREATE TABLE test_spatial_bbox_nothing_result
(
    a Polygon,
    v Variant(LineString),
    INDEX idx_bbox_a a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- The only granule sits near (100, 100), far from the (0, 0) point below.
INSERT INTO test_spatial_bbox_nothing_result
SELECT [[(100., 100.), (110., 100.), (110., 110.), (100., 100.)]],
       CAST([(0., 0.), (1., 1.)], 'LineString')
FROM numbers(4);

SET short_circuit_function_evaluation = 'disable';
SET optimize_move_to_prewhere = 0;

-- Every alternative of `v` is a `LineString`, which `pointInPolygon` rejects in its first argument,
-- so the node resolves to `Nullable(Nothing)` and cannot raise.
SELECT count() FROM test_spatial_bbox_nothing_result
WHERE pointInPolygon(v, 'x') AND pointInPolygon((0., 0.), a)
SETTINGS variant_throw_on_type_mismatch = 0;

-- With the default `variant_throw_on_type_mismatch = 1` the adaptor raises instead, so pruning must
-- stay off and the exception must surface rather than become a silent `0`.
SELECT count() FROM test_spatial_bbox_nothing_result
WHERE pointInPolygon(v, 'x')
  AND pointInPolygon((0., 0.), a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE test_spatial_bbox_nothing_result;
