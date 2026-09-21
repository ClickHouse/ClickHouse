-- Tags: no-parallel

SET enable_analyzer = 1;

-- Empty from section

SELECT 'Empty from section';

DESCRIBE (SELECT dummy);
SELECT dummy;

SELECT '--';

DESCRIBE (SELECT one.dummy);
SELECT one.dummy;

SELECT '--';

DESCRIBE (SELECT system.one.dummy);
SELECT system.one.dummy;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Table access without table name qualification';

SELECT test_id FROM test_table; -- { serverError UNKNOWN_IDENTIFIER }
SELECT test_id FROM test_unknown_table; -- { serverError UNKNOWN_TABLE }

DESCRIBE (SELECT id FROM test_table);
SELECT id FROM test_table;

SELECT '--';

DESCRIBE (SELECT value FROM test_table);
SELECT value FROM test_table;

SELECT '--';

DESCRIBE (SELECT id, value FROM test_table);
SELECT id, value FROM test_table;

SELECT 'Table access with table name qualification';

DESCRIBE (SELECT test_table.id FROM test_table);
SELECT test_table.id FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.value FROM test_table);
SELECT test_table.value FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM test_table);
SELECT test_table.id, test_table.value FROM test_table;

SELECT '--';

DESCRIBE (SELECT test.id, test.value FROM test_table AS test);
SELECT test.id, test.value FROM test_table AS test;

DROP TABLE test_table;

SELECT 'Table access with database and table name qualification';

DROP DATABASE IF EXISTS 02337_db;
CREATE DATABASE 02337_db;

DROP TABLE IF EXISTS 02337_db.test_table;
CREATE TABLE 02337_db.test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO 02337_db.test_table VALUES (0, 'Value');

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM 02337_db.test_table);
SELECT test_table.id, test_table.value FROM 02337_db.test_table;

SELECT '--';

DESCRIBE (SELECT 02337_db.test_table.id, 02337_db.test_table.value FROM 02337_db.test_table);
SELECT 02337_db.test_table.id, 02337_db.test_table.value FROM 02337_db.test_table;

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM 02337_db.test_table AS test_table);
SELECT test_table.id, test_table.value FROM 02337_db.test_table AS test_table;

DROP TABLE 02337_db.test_table;
DROP DATABASE 02337_db;
