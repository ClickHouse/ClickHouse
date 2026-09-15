-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the second half exercises `StorageReplicatedMergeTree::alter`, which
-- SharedMergeTree replaces with its own implementation.

-- Tests that a mixed `ALTER` that includes `MODIFY SETTING escape_index_filenames` takes effect
-- immediately. In such batches `changeSettings` commits the new policy, but the metadata copy that
-- the rest of the `ALTER` republishes was taken before `changeSettings` (`commands.apply` never
-- sets the escape fields), so the in-memory metadata used to flip back to the old filename policy
-- until a server restart.

DROP TABLE IF EXISTS t_escape_mixed_alter;

CREATE TABLE t_escape_mixed_alter
(
    a UInt64,
    INDEX `idx_ESPAÑA` a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, escape_index_filenames = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_escape_mixed_alter SELECT number FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

SELECT 'MergeTree, escaped filenames';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_mixed_alter' AND active;

-- The comment change routes the batch through the general `ALTER` path, which used to republish
-- the stale filename policy after `changeSettings` had already committed the new one.
ALTER TABLE t_escape_mixed_alter MODIFY COMMENT 'mixed alter', MODIFY SETTING escape_index_filenames = 0;

SELECT 'MergeTree, after the mixed ALTER';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_mixed_alter' AND active;

DROP TABLE t_escape_mixed_alter;

DROP TABLE IF EXISTS t_escape_mixed_alter_replicated;

CREATE TABLE t_escape_mixed_alter_replicated
(
    a UInt64,
    INDEX `idx_ESPAÑA` a TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04700_escape_mixed_alter', 'r1') ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, escape_index_filenames = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_escape_mixed_alter_replicated SELECT number FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

SELECT 'ReplicatedMergeTree, escaped filenames';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_mixed_alter_replicated' AND active;

-- A replicated command in the batch routes it through the replicated `ALTER` path, whose local
-- settings-and-comment commit used to republish the stale filename policy.
ALTER TABLE t_escape_mixed_alter_replicated ADD COLUMN b UInt8, MODIFY COMMENT 'mixed alter', MODIFY SETTING escape_index_filenames = 0
SETTINGS alter_sync = 1;

SELECT 'ReplicatedMergeTree, after the mixed ALTER';
SELECT DISTINCT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_escape_mixed_alter_replicated' AND active;

DROP TABLE t_escape_mixed_alter_replicated;
