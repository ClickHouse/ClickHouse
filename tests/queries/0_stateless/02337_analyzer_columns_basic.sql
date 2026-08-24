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

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.test_table;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.test_table VALUES (0, 'Value');

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table);
SELECT test_table.id, test_table.value FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table;

SELECT '--';

DESCRIBE (SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.id, {CLICKHOUSE_DATABASE_1:Identifier}.test_table.value FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table);
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.id, {CLICKHOUSE_DATABASE_1:Identifier}.test_table.value FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table;

SELECT '--';

DESCRIBE (SELECT test_table.id, test_table.value FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table AS test_table);
SELECT test_table.id, test_table.value FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table AS test_table;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_table;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
