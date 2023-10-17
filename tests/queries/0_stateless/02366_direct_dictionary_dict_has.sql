
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
   id UInt64,
   value String
)
ENGINE = Memory;

DROP TABLE IF EXISTS test_lookup_table;
CREATE TABLE test_lookup_table
(
   id UInt64,
   lookup_key UInt64,
)
ENGINE = Memory;

INSERT INTO test_table VALUES(0, 'value_0');

INSERT INTO test_lookup_table VALUES(0, 0);
INSERT INTO test_lookup_table VALUES(1, 0);
INSERT INTO test_lookup_table VALUES(2, 0);
INSERT INTO test_lookup_table VALUES(3, 1);
INSERT INTO test_lookup_table VALUES(4, 0);
INSERT INTO test_lookup_table VALUES(5, 1);
INSERT INTO test_lookup_table VALUES(6, 0);
INSERT INTO test_lookup_table VALUES(7, 2);
INSERT INTO test_lookup_table VALUES(8, 1);
INSERT INTO test_lookup_table VALUES(9, 0);

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary
(
   id UInt64,
   value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_table'))
LAYOUT(DIRECT());

SELECT id, lookup_key, dictHas('test_dictionary', lookup_key) FROM test_lookup_table ORDER BY id ASC;

DROP DICTIONARY test_dictionary;
DROP TABLE test_table;
DROP TABLE test_lookup_table;
