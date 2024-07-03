SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value');

-- { echoOn }

SELECT id, value_element, value FROM test_table ARRAY JOIN [[1,2,3]] AS value_element, value_element AS value;

SELECT id, value_element, value FROM test_table ARRAY JOIN [[1,2,3]] AS value_element ARRAY JOIN value_element AS value;

SELECT value_element, value FROM test_table ARRAY JOIN [1048577] AS value_element, arrayMap(x -> value_element, ['']) AS value;

SELECT arrayFilter(x -> notEmpty(concat(x)), [NULL, NULL]) FROM system.one ARRAY JOIN [1048577] AS elem, arrayMap(x -> concat(x, elem, ''), ['']) AS unused; -- { serverError 44 }

-- { echoOff }

DROP TABLE test_table;
