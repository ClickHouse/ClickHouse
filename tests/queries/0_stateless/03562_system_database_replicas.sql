-- Tags: no-parallel

DROP DATABASE IF EXISTS db_1 SYNC;
DROP DATABASE IF EXISTS db_2 SYNC;
DROP DATABASE IF EXISTS db_3 SYNC;
DROP DATABASE IF EXISTS db_4 SYNC;
DROP DATABASE IF EXISTS db_5 SYNC;
DROP DATABASE IF EXISTS db_6 SYNC;

SELECT '-----------------------';
SELECT 'simple SELECT';
CREATE DATABASE db_1 ENGINE = Replicated('/test/db_1', '{shard}', '{replica}');
CREATE DATABASE db_2 ENGINE = Replicated('/test/db_2', '{shard}', '{replica}');
CREATE DATABASE db_3 ENGINE = Replicated('/test/db_3', '{shard}', '{replica}');
CREATE DATABASE db_4 ENGINE = Replicated('/test/db_4', '{shard}', '{replica}');
CREATE DATABASE db_5 ENGINE = Replicated('/test/db_5', '{shard}', '{replica}');
CREATE DATABASE db_6 ENGINE = Replicated('/test/db_6', '{shard}', '{replica}');
SELECT sleep(1) FORMAT Null;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database;

SELECT database FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database;
SELECT DISTINCT is_readonly FROM system.database_replicas WHERE database LIKE 'db_%';

SELECT '-----------------------';
SELECT 'count';
SELECT count(*) FROM system.database_replicas WHERE database LIKE 'db_%';

SELECT '-----------------------';
SELECT 'SELECT with LIMIT';
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database LIMIT 1;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database LIMIT 6;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database LIMIT 7;

SELECT '-----------------------';
SELECT 'SELECT with max_block';
SET max_block_size=2;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database;

SET max_block_size=6;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database;

SELECT '-----------------------';
SELECT 'SELECT with WHERE';
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' AND is_readonly=0 ORDER BY database;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' AND is_readonly=1 ORDER BY database;
SELECT is_readonly FROM system.database_replicas WHERE database='db_2' ORDER BY database;
SELECT * FROM system.database_replicas WHERE database='db_11' ORDER BY database;

SELECT '-----------------------';
SELECT 'DROP DATABASE';
DROP DATABASE db_1;
SELECT * FROM system.database_replicas WHERE database LIKE 'db_%' ORDER BY database;

SELECT '-----------------------';
SELECT 'SELECT max_log_ptr';
SET distributed_ddl_output_mode='throw';
CREATE TABLE db_2.test_table (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT database, max_log_ptr FROM system.database_replicas WHERE database LIKE 'db_%' AND max_log_ptr > 1;
