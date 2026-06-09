SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics_for_part_pruning = 1;
-- Pin EXPLAIN-shape settings; `filterPartsByStatistics` swallows pruner exceptions in release builds,
-- so the result assertions cannot prove the pruner ran. Match `03788_statistics_part_pruning.sql`.
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

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

-- LowCardinality(Bool) key column vs LowCardinality(Nullable(UInt8)) constant (original AST fuzzer crash shape).
SELECT '-- LowCardinality(Bool) statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE b = toLowCardinality(toNullable(true))) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE b = toLowCardinality(toNullable(true)))
    WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM stid_1499_6033 WHERE b = toLowCardinality(toNullable(true));

-- Sibling shape: LowCardinality(UInt8) key column vs LowCardinality(Nullable(UInt8)) constant (ILLEGAL_COLUMN path).
SELECT '-- LowCardinality(UInt8) statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE v < toLowCardinality(toNullable(50))) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM stid_1499_6033 WHERE v < toLowCardinality(toNullable(50)))
    WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM stid_1499_6033 WHERE v < toLowCardinality(toNullable(50));

DROP TABLE stid_1499_6033;
