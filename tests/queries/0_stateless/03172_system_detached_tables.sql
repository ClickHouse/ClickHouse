-- Tags: no-parallel

DROP DATABASE IF EXISTS test_system_detached_tables;
CREATE DATABASE IF NOT EXISTS test_system_detached_tables;

CREATE TABLE test_system_detached_tables.test_table (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables;

DETACH TABLE test_system_detached_tables.test_table;
SELECT database, name, is_permanently FROM system.detached_tables;

ATTACH TABLE test_system_detached_tables.test_table;

CREATE TABLE test_system_detached_tables.test_table_perm (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables;

DETACH TABLE test_system_detached_tables.test_table_perm PERMANENTLY;
SELECT database, name, is_permanently FROM system.detached_tables;

DETACH TABLE test_system_detached_tables.test_table SYNC;
SELECT database, name, is_permanently FROM system.detached_tables;

SELECT database, name, is_permanently FROM system.detached_tables WHERE name='test_table';

DROP DATABASE test_system_detached_tables;



