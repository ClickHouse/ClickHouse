-- Regression test: `MergeTreeIndexConditionSpatialBbox::extractNodeBbox`
-- (src/Storages/MergeTree/MergeTreeIndexSpatialBbox.cpp) returns `NotApplicable` as soon as a
-- spatial predicate's indexed-column input isn't found among its children -- e.g. a conjunct
-- targeting a different geometry column entirely. That early return discards whether that
-- conjunct's constant geometry argument was actually extractable. With
-- `short_circuit_function_evaluation = 'disable'`, every conjunct of an `and` is always
-- evaluated, so a conjunct on `b` with an invalid constant polygon is guaranteed to raise
-- `BAD_ARGUMENTS` for every row -- but `collectConjunctiveBbox` would still happily keep pruning
-- using only the valid conjunct's bbox on the indexed column `a`, silently returning 0 rows
-- instead of raising the exception every row must raise.

DROP TABLE IF EXISTS test_spatial_bbox_and_conjunct_other_column;

CREATE TABLE test_spatial_bbox_and_conjunct_other_column
(
    id UInt32,
    a  Point,
    b  Point,
    INDEX idx_bbox a TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

-- All rows lie far outside the first conjunct's polygon, so an extractor that (incorrectly) prunes
-- using only the `a` conjunct's bbox returns 0 rows instead of raising `BAD_ARGUMENTS` from the
-- invalid constant polygon in the second (unrelated-column) conjunct.
INSERT INTO test_spatial_bbox_and_conjunct_other_column SELECT number + 1, (100., 100.), (0.5, 0.5) FROM numbers(8);

SELECT count() FROM test_spatial_bbox_and_conjunct_other_column
WHERE pointInPolygon(a, [(0., 0.), (1., 0.), (1., 1.), (0., 1.)])
  AND pointInPolygon(b, [(0., 0.), (1., 1.), (1., 0.), (0., 1.), (0., 0.)])
SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError BAD_ARGUMENTS }

DROP TABLE test_spatial_bbox_and_conjunct_other_column;
