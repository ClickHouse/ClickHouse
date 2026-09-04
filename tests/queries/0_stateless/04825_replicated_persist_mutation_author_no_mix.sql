-- Tags: zookeeper
-- `persist_mutation_author` gates the format of the shared `/mutations` entries in ClickHouse Keeper,
-- but setting changes are applied locally before the ZooKeeper transaction of a replicated `ALTER`
-- and are not rolled back if it fails. Mixing a change of this setting with replicated `ALTER`
-- commands is therefore rejected: a failed mixed `ALTER` could otherwise still switch the replica
-- onto the new entry format.

DROP TABLE IF EXISTS t_persist_author_no_mix SYNC;

CREATE TABLE t_persist_author_no_mix (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_persist_author_no_mix', '1')
ORDER BY id;

-- Mixing `MODIFY SETTING persist_mutation_author` with a replicated command is rejected.
ALTER TABLE t_persist_author_no_mix MODIFY COLUMN value LowCardinality(String), MODIFY SETTING persist_mutation_author = 1; -- { serverError BAD_ARGUMENTS }

-- Same for `RESET SETTING`.
ALTER TABLE t_persist_author_no_mix MODIFY COLUMN value LowCardinality(String), RESET SETTING persist_mutation_author; -- { serverError BAD_ARGUMENTS }

-- The rejected `ALTER` must not have applied the setting.
SELECT engine_full LIKE '%persist_mutation_author%'
FROM system.tables
WHERE database = currentDatabase() AND name = 't_persist_author_no_mix';

-- A standalone (purely local) settings `ALTER` works.
ALTER TABLE t_persist_author_no_mix MODIFY SETTING persist_mutation_author = 1;

-- Mixing it with other settings changes stays a purely local operation and is allowed.
ALTER TABLE t_persist_author_no_mix MODIFY SETTING persist_mutation_author = 0, max_parts_to_merge_at_once = 8;

-- Replicated commands without the setting keep working.
INSERT INTO t_persist_author_no_mix VALUES (1, 'a');
ALTER TABLE t_persist_author_no_mix MODIFY COLUMN value LowCardinality(String) SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT value FROM t_persist_author_no_mix;

DROP TABLE t_persist_author_no_mix SYNC;
