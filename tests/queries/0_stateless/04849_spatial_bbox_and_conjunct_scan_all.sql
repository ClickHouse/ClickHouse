-- Regression test: `MergeTreeIndexConditionSpatialBbox::extractQueryBbox` (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp)
-- walked the children of an `and` and returned as soon as ONE child yielded a usable bbox, never
-- looking at the remaining conjuncts. With `short_circuit_function_evaluation = 'disable'`, every
-- conjunct of an `and` is always evaluated regardless of the others' truth value. If pruning stops
-- at an earlier valid+prunable conjunct, a later conjunct whose constant geometry is invalid (and
-- therefore guaranteed to raise `BAD_ARGUMENTS` when evaluated) never gets a chance to force
-- `extractQueryBbox` to fail closed -- granules get pruned by the first conjunct's bbox alone, and
-- the query silently returns fewer rows (or 0) instead of raising the exception every row must raise.

DROP TABLE IF EXISTS test_spatial_bbox_and_conjunct_scan_all;

CREATE TABLE test_spatial_bbox_and_conjunct_scan_all
(
    id UInt32,
    p  Point,
    INDEX idx_bbox p TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

-- All rows lie inside the first (valid) conjunct's polygon, so a bbox extractor that stops at the
-- first conjunct would prune nothing here -- but it would also never reach the second, invalid
-- conjunct's bbox, so the granule wouldn't get pruned in a way that hides the exception either.
-- Rows instead lie OUTSIDE the second conjunct's tiny valid-looking region: an extractor that stops
-- at the first conjunct keeps every granule, evaluates the row-level predicate, and correctly raises
-- `BAD_ARGUMENTS` from the second (invalid) conjunct for every row -- so exercise the inverse: rows
-- inside the first conjunct only, first conjunct's bbox alone would NOT prune, ensuring any incorrect
-- pruning must come specifically from missing the second conjunct's `Failed` status.
INSERT INTO test_spatial_bbox_and_conjunct_scan_all SELECT number + 1, (0.5, 0.5) FROM numbers(8);

SELECT count() FROM test_spatial_bbox_and_conjunct_scan_all
WHERE pointInPolygon(p, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
  AND pointInPolygon(p, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_and_conjunct_scan_all;
