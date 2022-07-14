DROP TABLE IF EXISTS _02176_test_simple_key_table;
CREATE TABLE _02176_test_simple_key_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO _02176_test_simple_key_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS _02176_test_simple_key_dictionary;
CREATE DICTIONARY _02176_test_simple_key_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02176_test_simple_key_table'))
LAYOUT(DIRECT());

SELECT dictGet('_02176_test_simple_key_dictionary', 'value', toUInt64(0));
SELECT dictGet('_02176_test_simple_key_dictionary', 'value', toUInt8(0));
SELECT dictGet('_02176_test_simple_key_dictionary', 'value', '0');
SELECT dictGet('_02176_test_simple_key_dictionary', 'value', [0]); --{serverError 43}

SELECT dictHas('_02176_test_simple_key_dictionary', toUInt64(0));
SELECT dictHas('_02176_test_simple_key_dictionary', toUInt8(0));
SELECT dictHas('_02176_test_simple_key_dictionary', '0');
SELECT dictHas('_02176_test_simple_key_dictionary', [0]); --{serverError 43}

DROP DICTIONARY _02176_test_simple_key_dictionary;
DROP TABLE _02176_test_simple_key_table;

DROP TABLE IF EXISTS _02176_test_complex_key_table;
CREATE TABLE _02176_test_complex_key_table
(
    id UInt64,
    id_key String,
    value String
) ENGINE=TinyLog;

INSERT INTO _02176_test_complex_key_table VALUES (0, '0', 'Value');

DROP DICTIONARY IF EXISTS _02176_test_complex_key_dictionary;
CREATE DICTIONARY _02176_test_complex_key_dictionary
(
    id UInt64,
    id_key String,
    value String
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(TABLE '_02176_test_complex_key_table'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT dictGet('_02176_test_complex_key_dictionary', 'value', tuple(toUInt64(0), '0'));
SELECT dictGet('_02176_test_complex_key_dictionary', 'value', tuple(toUInt8(0), '0'));
SELECT dictGet('_02176_test_complex_key_dictionary', 'value', tuple('0', '0'));
SELECT dictGet('_02176_test_complex_key_dictionary', 'value', tuple([0], '0')); --{serverError 43}
SELECT dictGet('_02176_test_complex_key_dictionary', 'value', tuple(toUInt64(0), 0));

SELECT dictHas('_02176_test_complex_key_dictionary', tuple(toUInt64(0), '0'));
SELECT dictHas('_02176_test_complex_key_dictionary', tuple(toUInt8(0), '0'));
SELECT dictHas('_02176_test_complex_key_dictionary', tuple('0', '0'));
SELECT dictHas('_02176_test_complex_key_dictionary', tuple([0], '0')); --{serverError 43}
SELECT dictHas('_02176_test_complex_key_dictionary', tuple(toUInt64(0), 0));

DROP DICTIONARY _02176_test_complex_key_dictionary;
DROP TABLE _02176_test_complex_key_table;
