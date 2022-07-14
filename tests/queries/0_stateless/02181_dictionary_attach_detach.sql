DROP TABLE IF EXISTS _02181_test_table;
CREATE TABLE _02181_test_table
(
    id UInt64,
    value String
)
ENGINE = TinyLog;

INSERT INTO _02181_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS _02181_test_dictionary;
CREATE DICTIONARY _02181_test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02181_test_table'))
LAYOUT(HASHED())
LIFETIME(0);

DETACH TABLE _02181_test_dictionary; --{serverError 520}
ATTACH TABLE _02181_test_dictionary; --{serverError 80}

DETACH DICTIONARY _02181_test_dictionary;
ATTACH DICTIONARY _02181_test_dictionary;

SELECT * FROM _02181_test_dictionary;

DETACH DICTIONARY _02181_test_dictionary;
ATTACH DICTIONARY _02181_test_dictionary;

SELECT * FROM _02181_test_dictionary;

DETACH DICTIONARY _02181_test_dictionary;
ATTACH DICTIONARY _02181_test_dictionary;

DROP DICTIONARY _02181_test_dictionary;
DROP TABLE _02181_test_table;
