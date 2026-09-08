-- A partition key with several expressions over the same column, so that one predicate leaf emits
-- an exact atom (`x`) together with a relaxed sibling (`toUInt8(x)`). The sibling only narrows the
-- leaf further and does not make it inexact, so strict partition pruning - the metadata-only
-- `count()` over a partition predicate - has to stay available.

-- `count()` with a `WHERE` is answered from the partition metadata only by the old analyzer
-- (`totalRowsByPartitionPredicate`); the implicit `_minmax_count_projection` would hide it.
SET enable_analyzer = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS test_strict_partition_pruning;

CREATE TABLE test_strict_partition_pruning (x UInt32) ENGINE = MergeTree
PARTITION BY (toUInt8(x), x) ORDER BY tuple();

INSERT INTO test_strict_partition_pruning VALUES (1), (257), (513);

SELECT count() FROM test_strict_partition_pruning WHERE x = 257;

SELECT countIf(explain LIKE '%Optimized trivial count%') FROM (
    EXPLAIN SELECT count() FROM test_strict_partition_pruning WHERE x = 257);

-- The predicate that only the relaxed atom can answer keeps the exact atom of the other leaf, so
-- the fast path still applies, and the result stays correct.
SELECT count() FROM test_strict_partition_pruning WHERE toUInt8(x) = 1;

SELECT count() FROM test_strict_partition_pruning WHERE x != 257;

DROP TABLE test_strict_partition_pruning;
