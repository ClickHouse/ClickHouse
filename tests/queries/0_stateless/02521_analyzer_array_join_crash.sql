SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value');

-- { echoOn }

SELECT id, value_element, value FROM test_table ARRAY JOIN [[1,2,3]] AS value_element, value_element AS value; -- { serverError UNKNOWN_IDENTIFIER }

SELECT id, value_element, value FROM test_table ARRAY JOIN [[1,2,3]] AS value_element ARRAY JOIN value_element AS value;

SELECT value_element, value FROM test_table ARRAY JOIN [1048577] AS value_element ARRAY JOIN arrayMap(x -> value_element, ['']) AS value;

SELECT arrayFilter(x -> notEmpty(concat(x)), [NULL, NULL]) FROM system.one ARRAY JOIN [1048577] AS elem ARRAY JOIN arrayMap(x -> splitByChar(x, elem), ['']) AS unused; -- { serverError ILLEGAL_COLUMN }

-- { echoOff }

DROP TABLE test_table;
