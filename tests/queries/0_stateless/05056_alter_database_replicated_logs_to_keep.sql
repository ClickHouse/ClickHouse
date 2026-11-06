-- Tags: zookeeper, no-replicated-database, need-query-parameters
-- no-replicated-database: the test creates Replicated databases of its own

-- `logs_to_keep` is the one cluster-wide setting of a `Replicated` database: its effective value
-- lives in a shared ClickHouse Keeper node, seeded by the first replica. Two replicas of the same
-- database are created in this server to observe that the metadata files may disagree with it, and
-- that `ALTER DATABASE ... MODIFY SETTING` refuses to change it.

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

-- Selecting the column on its own must still fetch the Keeper-sourced fields, otherwise it would
-- silently report 0.
SELECT 'logs_to_keep alone', logs_to_keep FROM system.database_replicas
WHERE database = {CLICKHOUSE_DATABASE_2:String};

-- The metadata file of the second replica still records the discarded value, which is why
-- `SHOW CREATE DATABASE` cannot answer this question and `system.database_replicas` can.
SELECT 'metadata files', if(name = {CLICKHOUSE_DATABASE_1:String}, 'replica1', 'replica2'),
       extract(engine_full, 'logs_to_keep = (\\d+)')
FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY name;

-- Changing it is refused on any replica, alone or next to a replica-local setting, whose change is
-- not applied either.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING logs_to_keep = 777; -- { serverError QUERY_NOT_ALLOWED }
ALTER DATABASE {CLICKHOUSE_DATABASE_2:Identifier} MODIFY SETTING logs_to_keep = 777; -- { serverError QUERY_NOT_ALLOWED }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_broken_tables_ratio = 0.5, logs_to_keep = 777; -- { serverError QUERY_NOT_ALLOWED }

SELECT 'keeper node after rejected alters', value FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/logs_to_keep_db' AND name = 'logs_to_keep';

SELECT 'metadata files after rejected alters', if(name = {CLICKHOUSE_DATABASE_1:String}, 'replica1', 'replica2'),
       extract(engine_full, 'logs_to_keep = (\\d+)'), engine_full LIKE '%max_broken_tables_ratio%'
FROM system.databases WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
