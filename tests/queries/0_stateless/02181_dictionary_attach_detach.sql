DROP TABLE IF EXISTS 02181_test_table;
CREATE TABLE 02181_test_table
(
    id UInt64,
    value String
)
ENGINE = TinyLog;

INSERT INTO 02181_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02181_test_dictionary;
CREATE DICTIONARY 02181_test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02181_test_table'))
LAYOUT(HASHED())
LIFETIME(0);

DETACH TABLE 02181_test_dictionary; --{serverError 520}
ATTACH TABLE 02181_test_dictionary; --{serverError 80}

DETACH DICTIONARY 02181_test_dictionary;
ATTACH DICTIONARY 02181_test_dictionary;

SELECT * FROM 02181_test_dictionary;

DETACH DICTIONARY 02181_test_dictionary;
ATTACH DICTIONARY 02181_test_dictionary;

SELECT * FROM 02181_test_dictionary;

DETACH DICTIONARY 02181_test_dictionary;
ATTACH DICTIONARY 02181_test_dictionary;

DROP DICTIONARY 02181_test_dictionary;
DROP TABLE 02181_test_table;
