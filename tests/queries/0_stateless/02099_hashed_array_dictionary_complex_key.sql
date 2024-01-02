DROP TABLE IF EXISTS complex_key_simple_attributes_source_table;
CREATE TABLE complex_key_simple_attributes_source_table
(
   id UInt64,
   id_key String,
   value_first String,
   value_second String
)
ENGINE = TinyLog;

INSERT INTO complex_key_simple_attributes_source_table VALUES(0, 'id_key_0', 'value_0', 'value_second_0');
INSERT INTO complex_key_simple_attributes_source_table VALUES(1, 'id_key_1', 'value_1', 'value_second_1');
INSERT INTO complex_key_simple_attributes_source_table VALUES(2, 'id_key_2', 'value_2', 'value_second_2');

DROP DICTIONARY IF EXISTS hashed_array_dictionary_complex_key_simple_attributes;
CREATE DICTIONARY hashed_array_dictionary_complex_key_simple_attributes
(
   id UInt64,
   id_key String,
   value_first String DEFAULT 'value_first_default',
   value_second String DEFAULT 'value_second_default'
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(TABLE 'complex_key_simple_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_HASHED_ARRAY());

SELECT 'Dictionary hashed_array_dictionary_complex_key_simple_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('hashed_array_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', toString(number)))) as value_first,
    dictGet('hashed_array_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', toString(number)))) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('hashed_array_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', toString(number)))) as value_first,
    dictGet('hashed_array_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', toString(number)))) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('hashed_array_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', toString(number))), toString('default')) as value_first,
    dictGetOrDefault('hashed_array_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', toString(number))), toString('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('hashed_array_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', toString(number))), toString('default')) as value_first,
    dictGetOrDefault('hashed_array_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', toString(number))), toString('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('hashed_array_dictionary_complex_key_simple_attributes', (number, concat('id_key_', toString(number)))) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM hashed_array_dictionary_complex_key_simple_attributes ORDER BY (id, id_key);

DROP DICTIONARY hashed_array_dictionary_complex_key_simple_attributes;

DROP TABLE complex_key_simple_attributes_source_table;

DROP TABLE IF EXISTS complex_key_complex_attributes_source_table;
CREATE TABLE complex_key_complex_attributes_source_table
(
   id UInt64,
   id_key String,
   value_first String,
   value_second Nullable(String)
)
ENGINE = TinyLog;

INSERT INTO complex_key_complex_attributes_source_table VALUES(0, 'id_key_0', 'value_0', 'value_second_0');
INSERT INTO complex_key_complex_attributes_source_table VALUES(1, 'id_key_1', 'value_1', NULL);
INSERT INTO complex_key_complex_attributes_source_table VALUES(2, 'id_key_2', 'value_2', 'value_second_2');

DROP DICTIONARY IF EXISTS hashed_array_dictionary_complex_key_complex_attributes;
CREATE DICTIONARY hashed_array_dictionary_complex_key_complex_attributes
(
    id UInt64,
    id_key String,

    value_first String DEFAULT 'value_first_default',
    value_second Nullable(String) DEFAULT 'value_second_default'
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(TABLE 'complex_key_complex_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_HASHED_ARRAY());

SELECT 'Dictionary hashed_array_dictionary_complex_key_complex_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('hashed_array_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', toString(number)))) as value_first,
    dictGet('hashed_array_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', toString(number)))) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('hashed_array_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', toString(number)))) as value_first,
    dictGet('hashed_array_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', toString(number)))) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('hashed_array_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', toString(number))), toString('default')) as value_first,
    dictGetOrDefault('hashed_array_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', toString(number))), toString('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('hashed_array_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', toString(number))), toString('default')) as value_first,
    dictGetOrDefault('hashed_array_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', toString(number))), toString('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('hashed_array_dictionary_complex_key_complex_attributes', (number, concat('id_key_', toString(number)))) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM hashed_array_dictionary_complex_key_complex_attributes ORDER BY (id, id_key);

DROP DICTIONARY hashed_array_dictionary_complex_key_complex_attributes;
DROP TABLE complex_key_complex_attributes_source_table;
