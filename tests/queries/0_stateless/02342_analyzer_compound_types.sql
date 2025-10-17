SET enable_analyzer = 1;

SELECT 'Constant tuple';

SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, value.id, value.value;
SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, value.* APPLY toString;
SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, value.COLUMNS(id) APPLY toString;
SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, value.COLUMNS(value) APPLY toString;
SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, value.COLUMNS('i') APPLY toString;
SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, value.COLUMNS('v') APPLY toString;

SELECT 'Tuple';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value Tuple(value_0_level_0 Tuple(value_0_level_1 String, value_1_level_1 String), value_1_level_0 String)
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, (('value_0_level_1', 'value_1_level_1'), 'value_1_level_0'));

SELECT '--';

DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT '--';

DESCRIBE (SELECT id, value FROM test_table);
SELECT id, value FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table);
SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value AS alias_value, alias_value.value_0_level_0, alias_value.value_1_level_0 FROM test_table);
SELECT value AS alias_value, alias_value.value_0_level_0, alias_value.value_1_level_0 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value AS alias_value, alias_value.* FROM test_table);
SELECT value AS alias_value, alias_value.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value AS alias_value, alias_value.* APPLY toString FROM test_table);
SELECT value AS alias_value, alias_value.* APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.* FROM test_table);
SELECT value.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.* APPLY toString FROM test_table);
SELECT value.* APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table);
SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY toString FROM test_table);
SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* FROM test_table);
SELECT value.value_0_level_0.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* APPLY toString FROM test_table);
SELECT value.value_0_level_0.* APPLY toString FROM test_table;

DROP TABLE test_table;

-- SELECT 'Array of tuples';

-- DROP TABLE IF EXISTS test_table;
-- CREATE TABLE test_table
-- (
--     id UInt64,
--     value Array(Tuple(value_0_level_0 Tuple(value_0_level_1 String, value_1_level_1 String), value_1_level_0 String))
-- ) ENGINE=MergeTree ORDER BY id;

-- INSERT INTO test_table VALUES (0, [('value_0_level_1', 'value_1_level_1')], ['value_1_level_0']);

-- DESCRIBE (SELECT * FROM test_table);
-- SELECT * FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table);
-- SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table);
-- SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table);
-- SELECT value.value_0_level_0 AS alias_value, alias_value.value_0_level_1, alias_value.value_1_level_1 FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table);
-- SELECT value.value_0_level_0 AS alias_value, alias_value.* FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY toString FROM test_table);
-- SELECT value.value_0_level_0 AS alias_value, alias_value.* APPLY toString FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0.* FROM test_table);
-- SELECT value.value_0_level_0.* FROM test_table;

-- SELECT '--';

-- DESCRIBE (SELECT value.value_0_level_0.* APPLY toString FROM test_table);
-- SELECT value.value_0_level_0.* APPLY toString FROM test_table;

-- DROP TABLE test_table;

SELECT 'Nested';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value Nested (value_0_level_0 Nested(value_0_level_1 String, value_1_level_1 String), value_1_level_0 String)
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, [[('value_0_level_1', 'value_1_level_1')]], ['value_1_level_0']);

DESCRIBE (SELECT * FROM test_table);
SELECT * FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table);
SELECT value.value_0_level_0, value.value_1_level_0 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0.value_0_level_1, value.value_0_level_0.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS value_alias, value_alias.value_0_level_1, value_alias.value_1_level_1 FROM test_table);
SELECT value.value_0_level_0 AS value_alias, value_alias.value_0_level_1, value_alias.value_1_level_1 FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS value_alias, value_alias.* FROM test_table);
SELECT value.value_0_level_0 AS value_alias, value_alias.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0 AS value_alias, value_alias.* APPLY toString FROM test_table);
SELECT value.value_0_level_0 AS value_alias, value_alias.* APPLY toString FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* FROM test_table);
SELECT value.value_0_level_0.* FROM test_table;

SELECT '--';

DESCRIBE (SELECT value.value_0_level_0.* APPLY toString FROM test_table);
SELECT value.value_0_level_0.* APPLY toString FROM test_table;

DROP TABLE test_table;
