-- Tags: no-parallel, no-fasttest, no-flaky-check, no-encrypted-storage
-- Because we are creating a backup with fixed path.

DROP DATABASE IF EXISTS 03276_test_database;
CREATE DATABASE 03276_test_database;

CREATE TABLE 03276_test_database.test_table (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO 03276_test_database.test_table SELECT number, number FROM numbers(15000);

SELECT (id % 10) AS key, count() FROM 03276_test_database.test_table GROUP BY key ORDER BY key;

BACKUP TABLE 03276_test_database.test_table TO File('03276_test_database.test_table') FORMAT Null;

SELECT '--';

DROP DATABASE IF EXISTS 03276_test_table_backup_database;
CREATE DATABASE 03276_test_table_backup_database ENGINE = Backup('03276_test_database', File('03276_test_database.test_table'));

SELECT (id % 10) AS key, count() FROM 03276_test_table_backup_database.test_table GROUP BY key ORDER BY key;

DROP DATABASE 03276_test_table_backup_database;

DROP DATABASE 03276_test_database;
