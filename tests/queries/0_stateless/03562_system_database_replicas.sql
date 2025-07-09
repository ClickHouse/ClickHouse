SELECT '-----------------------';
SELECT 'simple SELECT';
CREATE DATABASE IF NOT EXISTS db_1 ENGINE = Replicated('/test/db_1', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS db_2 ENGINE = Replicated('/test/db_2', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS db_3 ENGINE = Replicated('/test/db_3', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS db_4 ENGINE = Replicated('/test/db_4', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS db_5 ENGINE = Replicated('/test/db_5', '{shard}', '{replica}');
CREATE DATABASE IF NOT EXISTS db_6 ENGINE = Replicated('/test/db_6', '{shard}', '{replica}');
SELECT * FROM system.database_replicas ORDER BY database;

SELECT database FROM system.database_replicas ORDER BY database;
SELECT DISTINCT is_readonly FROM system.database_replicas;

SELECT '-----------------------';
SELECT 'count';
SELECT count(*) FROM system.database_replicas;

SELECT '-----------------------';
SELECT 'SELECT with LIMIT';
SELECT * FROM system.database_replicas ORDER BY database LIMIT 1;
SELECT * FROM system.database_replicas ORDER BY database LIMIT 6;
SELECT * FROM system.database_replicas ORDER BY database LIMIT 7;

SELECT '-----------------------';
SELECT 'SELECT with max_block';
SET max_block_size=2;
SELECT * FROM system.database_replicas ORDER BY database;

SET max_block_size=6;
SELECT * FROM system.database_replicas ORDER BY database;

SELECT '-----------------------';
SELECT 'SELECT with WHERE';
SELECT * FROM system.database_replicas WHERE is_readonly=0 ORDER BY database;
SELECT * FROM system.database_replicas WHERE is_readonly=1 ORDER BY database;
SELECT is_readonly FROM system.database_replicas WHERE database='db_2' ORDER BY database;
SELECT * FROM system.database_replicas WHERE database='db_11' ORDER BY database;

SELECT '-----------------------';
SELECT 'DROP DATABASE';
DROP DATABASE db_1;
SELECT * FROM system.database_replicas ORDER BY database;