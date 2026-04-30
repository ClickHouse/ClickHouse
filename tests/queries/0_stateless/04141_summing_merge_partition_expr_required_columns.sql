-- Test: exercises `SummingMergeTree` merge with PARTITION BY (a % 2) ORDER BY (d + 0).
-- Covers: src/Processors/Merges/Algorithms/SummingSortedAlgorithm.cpp:357-362 and
--         src/Storages/MergeTree/MergeTreeDataWriter.cpp / MergeTask.cpp / ReadFromMergeTree.cpp
--         which now use `expression->getRequiredColumns()` instead of `column_names`.
-- The columns `a` and `d` are NOT in `column_names` of partition/sort key (those are `a % 2`
-- and `d + 0`) but ARE in the required columns of the expressions; they must NOT be summed.

DROP TABLE IF EXISTS test_summing_expr_keys;

CREATE TABLE test_summing_expr_keys (a Int64, d Int64, val Int64)
    ENGINE = SummingMergeTree PARTITION BY (a % 2) ORDER BY (d + 0)
    SETTINGS optimize_on_insert = 0;

SYSTEM STOP MERGES test_summing_expr_keys;

-- Two separate parts in the same partition (a % 2 = 1) with the same sort key (d + 0 = 5).
INSERT INTO test_summing_expr_keys VALUES (1, 5, 10);
INSERT INTO test_summing_expr_keys VALUES (1, 5, 20);

SELECT '--- before merge: 2 rows, separate parts ---';
SELECT a, d, val, _partition_id FROM test_summing_expr_keys ORDER BY a, d, val;

-- Path 1: SELECT FINAL goes through `addMergingFinal` in ReadFromMergeTree.cpp
-- which uses metadata_snapshot->getPartitionKey().expression->getRequiredColumns().
SELECT '--- SELECT FINAL ---';
SELECT a, d, val, _partition_id FROM test_summing_expr_keys FINAL ORDER BY a, d, val;

-- Path 2: OPTIMIZE FINAL triggers a background merge (MergeTask::createMergedStream)
-- which uses metadata_snapshot->getPartitionKey().expression->getRequiredColumns().
SYSTEM START MERGES test_summing_expr_keys;
OPTIMIZE TABLE test_summing_expr_keys FINAL;

SELECT '--- after OPTIMIZE FINAL ---';
SELECT a, d, val, _partition_id FROM test_summing_expr_keys ORDER BY a, d, val;

-- Path 3: insert-time merge through `MergeTreeDataWriter::mergeBlock`
-- (with default optimize_on_insert = 1 a single multi-row INSERT triggers it).
DROP TABLE test_summing_expr_keys;

CREATE TABLE test_summing_expr_keys (a Int64, d Int64, val Int64)
    ENGINE = SummingMergeTree PARTITION BY (a % 2) ORDER BY (d + 0);

INSERT INTO test_summing_expr_keys VALUES (1, 5, 10), (1, 5, 20), (2, 5, 100);

SELECT '--- after insert-time merge (optimize_on_insert=1) ---';
SELECT a, d, val, _partition_id FROM test_summing_expr_keys ORDER BY a, d, val;

DROP TABLE test_summing_expr_keys;
