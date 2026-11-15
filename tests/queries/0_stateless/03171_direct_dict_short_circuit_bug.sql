-- https://github.com/ClickHouse/ClickHouse/issues/65201
SET short_circuit_function_evaluation='enable';

DROP DICTIONARY IF EXISTS direct_dictionary_simple_key_simple_attributes;
DROP TABLE IF EXISTS simple_key_simple_attributes_source_table;

CREATE TABLE simple_key_simple_attributes_source_table
(
    id UInt64,
    value_first String,
    value_second String
)
    ENGINE = TinyLog;

INSERT INTO simple_key_simple_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO simple_key_simple_attributes_source_table VALUES(1, 'value_1', 'value_second_1');
INSERT INTO simple_key_simple_attributes_source_table VALUES(2, 'value_2', 'value_second_2');


CREATE DICTIONARY direct_dictionary_simple_key_simple_attributes
(
    `id` UInt64,
    `value_first` String DEFAULT 'value_first_default',
    `value_second` String DEFAULT 'value_second_default'
)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'simple_key_simple_attributes_source_table'))
    LAYOUT(DIRECT());

SELECT
    toUInt128(1),
    dictGetOrDefault('direct_dictionary_simple_key_simple_attributes', 'value_second', number, toString(toFixedString(toFixedString(toFixedString(materialize('default'), 7), 7), toUInt128(7)))) AS value_second
FROM system.numbers LIMIT 255
FORMAT Null;

DROP DICTIONARY IF EXISTS direct_dictionary_simple_key_simple_attributes;
DROP TABLE IF EXISTS simple_key_simple_attributes_source_table;
