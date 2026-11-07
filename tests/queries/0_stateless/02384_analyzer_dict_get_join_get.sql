SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(FLAT())
SOURCE(CLICKHOUSE(TABLE 'test_table'))
LIFETIME(0);

SELECT 'Dictionary';

SELECT * FROM test_dictionary;

SELECT dictGet('test_dictionary', 'value', toUInt64(0));

SELECT dictGet(test_dictionary, 'value', toUInt64(0));

WITH 'test_dictionary' AS dictionary SELECT dictGet(dictionary, 'value', toUInt64(0));

WITH 'invalid_dictionary' AS dictionary SELECT dictGet(dictionary, 'value', toUInt64(0)); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY test_dictionary;
DROP TABLE test_table;

DROP TABLE IF EXISTS test_table_join;
CREATE TABLE test_table_join
(
    id UInt64,
    value String
) ENGINE=Join(Any, Left, id);

INSERT INTO test_table_join VALUES (0, 'Value');

SELECT 'JOIN';

SELECT * FROM test_table_join;

SELECT joinGet('test_table_join', 'value', toUInt64(0));

SELECT joinGet(test_table_join, 'value', toUInt64(0));

WITH 'test_table_join' AS join_table SELECT joinGet(join_table, 'value', toUInt64(0));

WITH 'invalid_test_table_join' AS join_table SELECT joinGet(join_table, 'value', toUInt64(0)); -- { serverError UNKNOWN_TABLE }

DROP TABLE test_table_join;
