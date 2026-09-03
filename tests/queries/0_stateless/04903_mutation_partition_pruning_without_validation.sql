-- Tags: zookeeper
-- `validate_mutation_query = 0` must defer validation even when predicate-based mutation
-- partition pruning is enabled.

SET mutations_sync = 0;
SET optimize_mutations_with_partition_pruning = 1;
SET validate_mutation_query = 0;
SET allow_nondeterministic_mutations = 1;

DROP TABLE IF EXISTS mutation_pruning_without_validation;

CREATE TABLE mutation_pruning_without_validation
(
    d Date,
    x UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mutation_pruning_without_validation', 'r1')
PARTITION BY d
ORDER BY x;

INSERT INTO mutation_pruning_without_validation VALUES ('2026-01-01', 1);

-- The referenced table does not exist yet. With validation disabled the mutation must be
-- enqueued and retried later rather than rejected by the pruning analysis.
ALTER TABLE mutation_pruning_without_validation DELETE WHERE d IN (SELECT d FROM created_later);

-- The mutation entry is created in ZooKeeper synchronously by the ALTER, while
-- `system.mutations` is populated asynchronously by the mutations-updating task,
-- so only the former can be asserted without a race.
-- The path comes from `system.replicas`, not from the literal above: the engine arguments a user
-- writes are not necessarily the path the table ends up at.
SELECT count()
FROM system.zookeeper
WHERE path IN (
    SELECT zookeeper_path || '/mutations'
    FROM system.replicas
    WHERE database = currentDatabase() AND table = 'mutation_pruning_without_validation');

DROP TABLE mutation_pruning_without_validation SYNC;
