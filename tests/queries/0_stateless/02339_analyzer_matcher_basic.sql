-- Tags: no-parallel

SET enable_analyzer = 1;

SELECT 'Matchers without FROM section';

DESCRIBE (SELECT *);
SELECT *;

SELECT '--';

DESCRIBE (SELECT COLUMNS(dummy));
SELECT COLUMNS(dummy);

SELECT '--';

DESCRIBE (SELECT COLUMNS('d'));
SELECT COLUMNS('d');

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Unqualified matchers';

DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id) FROM test_table);
SELECT COLUMNS(id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id), COLUMNS(value) FROM test_table);
SELECT COLUMNS(id), COLUMNS(value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS('i'), COLUMNS('v') FROM test_table);
SELECT COLUMNS('i'), COLUMNS('v') FROM test_table;

SELECT 'Table qualified matchers';

DESCRIBE (SELECT test_table.* FROM test_table);
SELECT test_table.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.COLUMNS(id) FROM test_table);
SELECT test_table.COLUMNS(id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.COLUMNS(id), test_table.COLUMNS(value) FROM test_table);
SELECT test_table.COLUMNS(id), test_table.COLUMNS(value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT test_table.COLUMNS('i'), test_table.COLUMNS('v') FROM test_table);
SELECT test_table.COLUMNS('i'), test_table.COLUMNS('v') FROM test_table;

SELECT 'Database and table qualified matchers';

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

DESCRIBE (SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.* FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table);
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.* FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table;

SELECT '--';

DESCRIBE (SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS(id) FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table);
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS(id) FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table;

SELECT '--';

DESCRIBE (SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS(id), {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS(value) FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table);
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS(id), {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS(value) FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table;

SELECT '--';

DESCRIBE (SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS('i'), {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS('v') FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table);
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS('i'), {CLICKHOUSE_DATABASE_1:Identifier}.test_table.COLUMNS('v') FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_table;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_table;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'APPLY transformer';

SELECT '--';

DESCRIBE (SELECT * APPLY toString FROM test_table);
SELECT * APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT * APPLY (x -> toString(x)) FROM test_table);
SELECT * APPLY (x -> toString(x)) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * APPLY (x -> toString(x)) APPLY (x -> length(x)) FROM test_table);
SELECT * APPLY (x -> toString(x)) APPLY (x -> length(x)) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * APPLY (x -> toString(x)) APPLY length FROM test_table);
SELECT * APPLY (x -> toString(x)) APPLY length FROM test_table;

SELECT '--';
DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT 'EXCEPT transformer';

SELECT '--';

DESCRIBE (SELECT * EXCEPT (id) FROM test_table);
SELECT * EXCEPT (id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) EXCEPT (id) FROM test_table);
SELECT COLUMNS(id, value) EXCEPT (id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * EXCEPT (id) APPLY toString FROM test_table);
SELECT * EXCEPT (id) APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) EXCEPT (id) APPLY toString FROM test_table);
SELECT COLUMNS(id, value) EXCEPT (id) APPLY toString FROM test_table;

SELECT 'REPLACE transformer';

SELECT '--';

DESCRIBE (SELECT * REPLACE (5 AS id) FROM test_table);
SELECT * REPLACE (5 AS id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) REPLACE (5 AS id) FROM test_table);
SELECT COLUMNS(id, value) REPLACE (5 AS id) FROM test_table;

SELECT '--';

DESCRIBE (SELECT * REPLACE (5 AS id, 6 as value) FROM test_table);
SELECT * REPLACE (5 AS id, 6 as value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) REPLACE (5 AS id, 6 as value) FROM test_table);
SELECT COLUMNS(id, value) REPLACE (5 AS id, 6 as value) FROM test_table;

SELECT 'Combine EXCEPT, REPLACE, APPLY transformers';

SELECT '--';

DESCRIBE (SELECT * EXCEPT id REPLACE (5 AS id, 6 as value) APPLY toString FROM test_table);
SELECT * EXCEPT id REPLACE (5 AS id, 6 as value) APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT COLUMNS(id, value) EXCEPT id REPLACE (5 AS id, 6 as value) APPLY toString FROM test_table);
SELECT COLUMNS(id, value) EXCEPT id REPLACE (5 AS id, 6 as value) APPLY toString FROM test_table;
