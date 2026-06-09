-- Regression test for `Bad cast from type DB::ColumnLowCardinality to
-- DB::ColumnVector<char8_t>` in `StatisticsPartPruner::checkPartCanMatch`.
-- The CI report swallowed the exception in `MergeTreeDataSelectExecutor::filterPartsByStatistics`,
-- so plain result assertions cannot detect a regression: a still-buggy pruner
-- throws, the exception is caught, the part is kept, and `count()` returns the
-- same number. The check below uses `EXPLAIN indexes = 1` to assert the
-- statistics pruner actually ran and pruned the expected number of parts.
SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics_for_part_pruning = 1;

DROP TABLE IF EXISTS stid_1499_6033;

CREATE TABLE stid_1499_6033
(
    id Nullable(UInt32),
    b  LowCardinality(Bool) STATISTICS(minmax),
    v  LowCardinality(UInt8) STATISTICS(minmax)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, allow_nullable_key = 1;

-- Two non-overlapping parts so statistics minmax can prune one of them.
-- Part 1: b = true,  v in [1, 8]
-- Part 2: b = false, v in [100, 107]
INSERT INTO stid_1499_6033 SELECT number,     true,  number + 1   FROM numbers(8);
INSERT INTO stid_1499_6033 SELECT number+100, false, number + 100 FROM numbers(8);

-- Original AST fuzzer crash shape: LowCardinality(Bool) key column compared
-- against a LowCardinality(Nullable(UInt8)) constant. With the bug, the runtime
-- type forwarded to `applyMonotonicFunctionsChainToRange` is the original
-- LowCardinality type while the cast wrapper was prepared against the
-- LC-stripped type, and `createUInt8ToBoolWrapper` throws `Bad cast`. The fix
-- strips LowCardinality to match the convention the chain was built with.
-- The Statistics index entry must appear and prune Part 2 (only Part 1 has
-- `b = true`, so 1/2 parts remain).
SELECT '-- LowCardinality(Bool) statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE b = toLowCardinality(toNullable(true))) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE b = toLowCardinality(toNullable(true)))
    WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM stid_1499_6033 WHERE b = toLowCardinality(toNullable(true));

-- Sibling shape: LowCardinality(UInt8) key column compared against a
-- LowCardinality(Nullable(UInt8)) constant. Without the fix, the pruner would
-- throw `LowCardinality(UInt8) cannot be inside Nullable column. (ILLEGAL_COLUMN)`
-- via the same chain-vs-runtime-type mismatch path; with the fix, the chain
-- runs and prunes Part 2.
SELECT '-- LowCardinality(UInt8) statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE v < toLowCardinality(toNullable(50))) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE v < toLowCardinality(toNullable(50)))
    WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM stid_1499_6033 WHERE v < toLowCardinality(toNullable(50));

DROP TABLE stid_1499_6033;
