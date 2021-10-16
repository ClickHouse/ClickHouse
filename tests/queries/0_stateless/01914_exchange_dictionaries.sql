DROP TABLE IF EXISTS table_1;
CREATE TABLE table_1 (id UInt64, value String) ENGINE=TinyLog;

DROP TABLE IF EXISTS table_2;
CREATE TABLE table_2 (id UInt64, value String) ENGINE=TinyLog;

INSERT INTO table_1 VALUES (1, 'Table1');
INSERT INTO table_2 VALUES (2, 'Table2');

DROP DICTIONARY IF EXISTS dictionary_1;
CREATE DICTIONARY dictionary_1 (id UInt64, value String)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'table_1'));

DROP DICTIONARY IF EXISTS dictionary_2;
CREATE DICTIONARY dictionary_2 (id UInt64, value String)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'table_2'));

SELECT * FROM dictionary_1;
SELECT * FROM dictionary_2;

EXCHANGE DICTIONARIES dictionary_1 AND dictionary_2;

SELECT * FROM dictionary_1;
SELECT * FROM dictionary_2;

DROP DICTIONARY dictionary_1;
DROP DICTIONARY dictionary_2;

DROP TABLE table_1;
DROP TABLE table_2;
