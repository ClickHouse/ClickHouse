-- Tags: no-parallel, no-fasttest, no-flaky-check, no-encrypted-storage
-- Because we are creating a backup with fixed path.

DROP DATABASE IF EXISTS 03279_test_database;
CREATE DATABASE 03279_test_database;

CREATE TABLE 03279_test_database.test_table_1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO 03279_test_database.test_table_1 SELECT number, number FROM numbers(15000);

CREATE TABLE 03279_test_database.test_table_2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO 03279_test_database.test_table_2 SELECT number, number FROM numbers(15000);

SELECT (id % 10) AS key, count() FROM 03279_test_database.test_table_1 GROUP BY key ORDER BY key;

SELECT '--';

SELECT (id % 10) AS key, count() FROM 03279_test_database.test_table_2 GROUP BY key ORDER BY key;

BACKUP DATABASE 03279_test_database TO Disk('backups', '03279_test_database') FORMAT Null;

SELECT '--';

DROP DATABASE IF EXISTS 03279_test_database_backup_database;
CREATE DATABASE 03279_test_database_backup_database ENGINE = Backup('03279_test_database', Disk('backups', '03279_test_database'));

SELECT name, total_rows FROM system.tables WHERE database = '03279_test_database_backup_database' ORDER BY name;

SELECT '--';

SELECT (id % 10) AS key, count() FROM 03279_test_database_backup_database.test_table_1 GROUP BY key ORDER BY key;

SELECT '--';

SELECT (id % 10) AS key, count() FROM 03279_test_database_backup_database.test_table_2 GROUP BY key ORDER BY key;

DROP DATABASE 03279_test_database_backup_database;

DROP DATABASE 03279_test_database;
