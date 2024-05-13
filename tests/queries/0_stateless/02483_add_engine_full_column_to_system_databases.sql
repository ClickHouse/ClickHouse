-- Tags: no-parallel

DROP DATABASE IF EXISTS replicated_database_test; 
CREATE DATABASE IF NOT EXISTS replicated_database_test ENGINE = Replicated('some/path/' || currentDatabase() || '/replicated_database_test', 'shard_1', 'replica_1') SETTINGS max_broken_tables_ratio=1;
SELECT engine_full FROM system.databases WHERE name = 'replicated_database_test';
DROP DATABASE IF EXISTS replicated_database_test;
