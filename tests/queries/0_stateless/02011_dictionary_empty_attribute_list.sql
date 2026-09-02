-- Tags: no-parallel

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (id UInt64) ENGINE=TinyLog;
INSERT INTO test_table VALUES (0);

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary (id UInt64) PRIMARY KEY id LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(TABLE 'test_table'));
SELECT * FROM test_dictionary;
SELECT dictHas('test_dictionary', toUInt64(0));
SELECT dictHas('test_dictionary', toUInt64(1));

DROP DICTIONARY test_dictionary;
DROP TABLE test_table;
