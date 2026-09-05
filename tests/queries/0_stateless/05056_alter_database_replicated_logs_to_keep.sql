-- Tags: zookeeper, no-replicated-database, need-query-parameters, no-parallel
-- no-replicated-database: the test creates Replicated databases of its own
-- no-parallel: a fail point is enabled, and fail points are global server state

-- `logs_to_keep` is the one cluster-wide setting of a `Replicated` database: its effective value
-- lives in a shared ClickHouse Keeper node, so an `ALTER` on one replica changes the behaviour of
-- every replica even though the statement itself is local. Two replicas of the same database are
-- created in this server to observe that.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Replicated('/test/' || currentDatabase() || '/logs_to_keep_db', 'shard1', 'replica1')
SETTINGS logs_to_keep = 1234;

CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier}
ENGINE = Replicated('/test/' || currentDatabase() || '/logs_to_keep_db', 'shard1', 'replica2')
SETTINGS logs_to_keep = 4321;

-- The first replica seeds the shared node; the value in a later replica's `CREATE` is discarded.
SELECT 'keeper node after create', value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/logs_to_keep_db' AND name = 'logs_to_keep';

SELECT 'after create', replica_name, logs_to_keep FROM system.database_replicas
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY replica_name;

-- Changing it on one replica changes the effective value for the other, with no restart and no
-- reinitialization of the other replica: the value is never cached, it is read from Keeper where
-- it is consumed.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 777;

SELECT 'keeper node after alter', value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/logs_to_keep_db' AND name = 'logs_to_keep';

SELECT 'after alter', replica_name, logs_to_keep FROM system.database_replicas
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY replica_name;

-- Selecting the column on its own must still fetch the Keeper-sourced fields, otherwise it would
-- silently report 0.
SELECT 'logs_to_keep alone', logs_to_keep FROM system.database_replicas
WHERE database = {CLICKHOUSE_DATABASE_2:String};

-- Only the replica the `ALTER` ran on records the new value in its metadata file, which is why
-- `SHOW CREATE DATABASE` cannot answer this question and `system.database_replicas` can.
SELECT 'metadata files', if(name = {CLICKHOUSE_DATABASE_1:String}, 'replica1', 'replica2'),
       extract(engine_full, 'logs_to_keep = (\\d+)')
FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY name;

-- Above the 32-bit domain of DDL log entry numbers.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 4294967296; -- { serverError BAD_ARGUMENTS }
-- Rejected by the setting's own type.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 0; -- { serverError BAD_ARGUMENTS }
-- The fallible preparation of the other change runs before the Keeper write, so the node is not
-- touched even though `logs_to_keep` comes first in the statement.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 999, collection_name = 'no_such_collection'; -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

SELECT 'keeper node after rejected alters', value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/logs_to_keep_db' AND name = 'logs_to_keep';

SELECT 'after rejected alters', replica_name, logs_to_keep FROM system.database_replicas
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY replica_name;

-- Keeper is the behavioural truth for `logs_to_keep`, so it is written first. If the metadata
-- file cannot be written afterwards, the change is already in effect on every replica and only
-- this replica's `SHOW CREATE DATABASE` is left behind. The statement fails, and re-running it
-- reconciles the file.
SYSTEM ENABLE FAILPOINT database_replicated_alter_settings_fail_after_keeper_write;
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 555; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT database_replicated_alter_settings_fail_after_keeper_write;

SELECT 'keeper node after failed metadata write', value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/logs_to_keep_db' AND name = 'logs_to_keep';

SELECT 'after failed metadata write', replica_name, logs_to_keep FROM system.database_replicas
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY replica_name;

SELECT 'metadata file after failed metadata write', extract(engine_full, 'logs_to_keep = (\\d+)')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 555;

SELECT 'metadata file after re-running', extract(engine_full, 'logs_to_keep = (\\d+)')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
