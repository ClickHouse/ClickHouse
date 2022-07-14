DROP TABLE IF EXISTS _02188_test_dictionary_source;
CREATE TABLE _02188_test_dictionary_source
(
    id UInt64,
    value String
)
ENGINE=TinyLog;

INSERT INTO _02188_test_dictionary_source VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS _02188_test_dictionary_simple_primary_key;
CREATE DICTIONARY _02188_test_dictionary_simple_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02188_test_dictionary_source'))
LAYOUT(DIRECT());

SELECT 'Dictionary output';
SELECT * FROM _02188_test_dictionary_simple_primary_key;
DROP DICTIONARY _02188_test_dictionary_simple_primary_key;

CREATE DICTIONARY _02188_test_dictionary_simple_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY (id)
SOURCE(CLICKHOUSE(TABLE '_02188_test_dictionary_source'))
LAYOUT(DIRECT());

SELECT 'Dictionary output';
SELECT * FROM _02188_test_dictionary_simple_primary_key;
DROP DICTIONARY _02188_test_dictionary_simple_primary_key;

DROP DICTIONARY IF EXISTS _02188_test_dictionary_complex_primary_key;
CREATE DICTIONARY _02188_test_dictionary_complex_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY id, value
SOURCE(CLICKHOUSE(TABLE '_02188_test_dictionary_source'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Dictionary output';
SELECT * FROM _02188_test_dictionary_complex_primary_key;
DROP DICTIONARY _02188_test_dictionary_complex_primary_key;

CREATE DICTIONARY _02188_test_dictionary_complex_primary_key
(
    id UInt64,
    value String
)
PRIMARY KEY (id, value)
SOURCE(CLICKHOUSE(TABLE '_02188_test_dictionary_source'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Dictionary output';
SELECT * FROM _02188_test_dictionary_complex_primary_key;
DROP DICTIONARY _02188_test_dictionary_complex_primary_key;

DROP TABLE _02188_test_dictionary_source;
