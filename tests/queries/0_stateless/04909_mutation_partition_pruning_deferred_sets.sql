-- Tags: zookeeper
-- A deferred set must not be evaluated while selecting the mutation partitions.

SET mutations_sync = 0;
SET optimize_mutations_with_partition_pruning = 1;
SET validate_mutation_query = 0;
SET allow_nondeterministic_mutations = 1;

DROP TABLE IF EXISTS mutation_pruning_deferred_sets;

CREATE TABLE mutation_pruning_deferred_sets
(
    d UInt64,
    x UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mutation_pruning_deferred_sets', 'r1')
PARTITION BY d
ORDER BY x;

INSERT INTO mutation_pruning_deferred_sets VALUES (1, 1);

-- The table does not exist yet. Its set must be deferred to mutation execution, just like an
-- explicit subquery, rather than being evaluated by the pruning pass.
ALTER TABLE mutation_pruning_deferred_sets DELETE WHERE d IN mutation_pruning_deferred_set_source;
ALTER TABLE mutation_pruning_deferred_sets DELETE WHERE d IN numbers(2);

DROP TABLE mutation_pruning_deferred_sets;
