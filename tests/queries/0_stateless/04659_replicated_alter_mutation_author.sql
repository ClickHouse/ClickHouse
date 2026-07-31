-- Tags: zookeeper
-- Replicated `ALTER` statements that enqueue a background mutation go through
-- `StorageReplicatedMergeTree::alter`, not through `StorageReplicatedMergeTree::mutate`,
-- so the `author` column has to be populated there as well.

DROP TABLE IF EXISTS t_replicated_alter_mutation_author SYNC;

CREATE TABLE t_replicated_alter_mutation_author
(
    id UInt64,
    value String,
    INDEX idx_value value TYPE set(100) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_replicated_alter_mutation_author', '1')
ORDER BY id
SETTINGS persist_mutation_author = 1;

INSERT INTO t_replicated_alter_mutation_author VALUES (1, 'a'), (2, 'b');

-- `MODIFY COLUMN` with a type change produces a background mutation.
ALTER TABLE t_replicated_alter_mutation_author MODIFY COLUMN value LowCardinality(String) SETTINGS mutations_sync = 2, alter_sync = 2;

-- `MATERIALIZE INDEX` produces a background mutation as well.
ALTER TABLE t_replicated_alter_mutation_author MATERIALIZE INDEX idx_value SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT command, is_done, author = currentUser() AS author_is_current_user
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_replicated_alter_mutation_author'
ORDER BY mutation_id;

-- The author survives a reload of the mutation entries from ClickHouse Keeper.
DETACH TABLE t_replicated_alter_mutation_author;
ATTACH TABLE t_replicated_alter_mutation_author;

SELECT command, author = currentUser() AS author_is_current_user
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_replicated_alter_mutation_author'
ORDER BY mutation_id;

-- Without `persist_mutation_author` the author is not recorded for this path either.
DROP TABLE IF EXISTS t_replicated_alter_mutation_author_disabled SYNC;

CREATE TABLE t_replicated_alter_mutation_author_disabled (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_replicated_alter_mutation_author_disabled', '1')
ORDER BY id;

INSERT INTO t_replicated_alter_mutation_author_disabled VALUES (1, 'a');

ALTER TABLE t_replicated_alter_mutation_author_disabled MODIFY COLUMN value LowCardinality(String) SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT command, is_done, author
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_replicated_alter_mutation_author_disabled'
ORDER BY mutation_id;

DROP TABLE t_replicated_alter_mutation_author SYNC;
DROP TABLE t_replicated_alter_mutation_author_disabled SYNC;
