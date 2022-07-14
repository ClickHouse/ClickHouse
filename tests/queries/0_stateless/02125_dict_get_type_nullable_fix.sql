DROP TABLE IF EXISTS _02125_test_table;
CREATE TABLE _02125_test_table
(
    id UInt64,
    value Nullable(String)
)
ENGINE=TinyLog;

INSERT INTO _02125_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS _02125_test_dictionary;
CREATE DICTIONARY _02125_test_dictionary
(
    id UInt64,
    value Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02125_test_table'))
LAYOUT(DIRECT());

SELECT dictGet('_02125_test_dictionary', 'value', toUInt64(0));
SELECT dictGetString('_02125_test_dictionary', 'value', toUInt64(0)); --{serverError 53}
