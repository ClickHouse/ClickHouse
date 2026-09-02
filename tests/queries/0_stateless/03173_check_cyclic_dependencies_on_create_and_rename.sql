-- Tags: atomic-database

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
INSERT INTO test SELECT number, 'str_' || toString(number) FROM numbers(10);
DROP DICTIONARY IF EXISTS test_dict;
CREATE DICTIONARY test_dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE test))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);
DROP TABLE IF EXISTS view_source;
CREATE TABLE view_source (id UInt64) ENGINE=MergeTree ORDER BY id;
INSERT INTO view_source SELECT * FROM numbers(5);
DROP VIEW IF EXISTS view;
CREATE VIEW view AS SELECT id, dictGet('test_dict', 'value', id) as value FROM view_source;

CREATE OR REPLACE DICTIONARY test_dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE view))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000); -- {serverError INFINITE_LOOP}

REPLACE DICTIONARY test_dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE view))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000); -- {serverError INFINITE_LOOP}


DROP DICTIONARY IF EXISTS test_dict_2;
CREATE DICTIONARY test_dict_2
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE view))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

EXCHANGE DICTIONARIES test_dict AND test_dict_2; -- {serverError INFINITE_LOOP}

DROP DICTIONARY test_dict_2;

CREATE OR REPLACE DICTIONARY test_dict_2
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE view))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000); 

EXCHANGE DICTIONARIES test_dict AND test_dict_2; -- {serverError INFINITE_LOOP}

DROP DICTIONARY test_dict;
RENAME DICTIONARY test_dict_2 to test_dict; -- {serverError INFINITE_LOOP}

DROP DICTIONARY test_dict_2;
DROP VIEW view;
DROP TABLE test;
DROP TABLE view_source;

