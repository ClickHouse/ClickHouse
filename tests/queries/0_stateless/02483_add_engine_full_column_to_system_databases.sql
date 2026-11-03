DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE:Identifier} ENGINE = Replicated('some/path/' || currentDatabase() || '/replicated_database_test', 'shard_1', 'replica_1') SETTINGS max_broken_tables_ratio=1;
SELECT engine_full FROM system.databases WHERE name = current_database();
