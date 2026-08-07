DROP TABLE IF EXISTS 02125_test_table;
CREATE TABLE 02125_test_table
(
    id UInt64,
    value Nullable(String)
)
ENGINE=TinyLog;

INSERT INTO 02125_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02125_test_dictionary;
CREATE DICTIONARY 02125_test_dictionary
(
    id UInt64,
    value Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02125_test_table'))
LAYOUT(DIRECT());

SELECT dictGet('02125_test_dictionary', 'value', toUInt64(0));
SELECT dictGetString('02125_test_dictionary', 'value', toUInt64(0)); --{serverError TYPE_MISMATCH}
