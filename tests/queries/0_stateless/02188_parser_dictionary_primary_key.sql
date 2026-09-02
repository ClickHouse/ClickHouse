DROP TABLE IF EXISTS 02188_test_dictionary_source;
CREATE TABLE 02188_test_dictionary_source
(
    id UInt64,
    value String
)
ENGINE=TinyLog;

INSERT INTO 02188_test_dictionary_source VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02188_test_dictionary_simple_primary_key;
CREATE DICTIONARY 02188_test_dictionary_simple_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02188_test_dictionary_source'))
LAYOUT(DIRECT());

SELECT 'Dictionary output';
SELECT * FROM 02188_test_dictionary_simple_primary_key;
DROP DICTIONARY 02188_test_dictionary_simple_primary_key;

CREATE DICTIONARY 02188_test_dictionary_simple_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY (id)
SOURCE(CLICKHOUSE(TABLE '02188_test_dictionary_source'))
LAYOUT(DIRECT());

SELECT 'Dictionary output';
SELECT * FROM 02188_test_dictionary_simple_primary_key;
DROP DICTIONARY 02188_test_dictionary_simple_primary_key;

DROP DICTIONARY IF EXISTS 02188_test_dictionary_complex_primary_key;
CREATE DICTIONARY 02188_test_dictionary_complex_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY id, value
SOURCE(CLICKHOUSE(TABLE '02188_test_dictionary_source'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Dictionary output';
SELECT * FROM 02188_test_dictionary_complex_primary_key;
DROP DICTIONARY 02188_test_dictionary_complex_primary_key;

CREATE DICTIONARY 02188_test_dictionary_complex_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY (id, value)
SOURCE(CLICKHOUSE(TABLE '02188_test_dictionary_source'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Dictionary output';
SELECT * FROM 02188_test_dictionary_complex_primary_key;
DROP DICTIONARY 02188_test_dictionary_complex_primary_key;

DROP TABLE 02188_test_dictionary_source;
