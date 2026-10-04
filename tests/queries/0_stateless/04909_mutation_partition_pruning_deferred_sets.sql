-- Tags: zookeeper
-- A deferred set must not be evaluated while selecting the mutation partitions.

SET mutations_sync = 0;
SET optimize_mutations_with_partition_pruning = 1;
SET allow_nondeterministic_mutations = 1;

DROP TABLE IF EXISTS mutation_pruning_deferred_sets;
DROP TABLE IF EXISTS mutation_pruning_deferred_set_source;

CREATE TABLE mutation_pruning_deferred_sets
(
    d UInt64,
    x UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mutation_pruning_deferred_sets', 'r1')
PARTITION BY d
ORDER BY x;

INSERT INTO mutation_pruning_deferred_sets VALUES (1, 1);

CREATE TABLE mutation_pruning_deferred_set_source (d UInt64) ENGINE = MergeTree ORDER BY d;
INSERT INTO mutation_pruning_deferred_set_source VALUES (1);

-- A table on the right-hand side of `IN` is a prepared set, just like an explicit subquery.
-- It must be deferred to mutation execution rather than being evaluated by the pruning pass.
-- (The third form the analyzer turns into a prepared set, a table function, cannot be tested
-- here: a mutation predicate is analyzed as an expression, so `IN numbers(2)` is rejected with
-- `UNKNOWN_FUNCTION` before any pruning happens.)
ALTER TABLE mutation_pruning_deferred_sets DELETE WHERE d IN mutation_pruning_deferred_set_source;

DROP TABLE mutation_pruning_deferred_sets;
DROP TABLE mutation_pruning_deferred_set_source;
