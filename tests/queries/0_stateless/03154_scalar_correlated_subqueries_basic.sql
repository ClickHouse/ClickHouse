SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value_0');
INSERT INTO test_table VALUES (1, 'Value_1');
INSERT INTO test_table VALUES (2, 'Value_2');

-- { echoOn }

SELECT (SELECT id) FROM test_table ORDER BY id;

SELECT '--';

SELECT (SELECT test_table.id) FROM test_table ORDER BY id;

SELECT '--';

SELECT (SELECT value) FROM test_table ORDER BY id;

SELECT '--';

SELECT (SELECT test_table.value) FROM test_table ORDER BY id;

SELECT '--';

SELECT (SELECT test_table.id + length(test_table.value)) FROM test_table ORDER BY id;

SELECT '--';

SELECT (SELECT test_table.id, test_table.value) FROM test_table ORDER BY id;

-- { echoOff }

DROP TABLE test_table;
