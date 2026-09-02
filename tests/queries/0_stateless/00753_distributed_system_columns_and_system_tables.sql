-- Tags: distributed

SELECT 'Check total_bytes/total_rows for Distributed';
CREATE TABLE check_system_tables_null (key Int) Engine=Null();
CREATE TABLE check_system_tables AS check_system_tables_null Engine=Distributed(test_shard_localhost, currentDatabase(), check_system_tables_null);
SYSTEM STOP DISTRIBUTED SENDS check_system_tables;
SELECT total_bytes, total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'check_system_tables';
INSERT INTO check_system_tables SELECT * FROM numbers(1) SETTINGS prefer_localhost_replica=0;
SELECT total_bytes>0, total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'check_system_tables';
SYSTEM FLUSH DISTRIBUTED check_system_tables;
SELECT total_bytes, total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'check_system_tables';
DROP TABLE check_system_tables_null;
DROP TABLE check_system_tables;
