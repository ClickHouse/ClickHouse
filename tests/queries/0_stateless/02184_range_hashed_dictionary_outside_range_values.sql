DROP TABLE IF EXISTS 02184_range_dictionary_source_table;
CREATE TABLE 02184_range_dictionary_source_table
(
    id UInt64,
    start UInt64,
    end UInt64,
    value_0 String,
    value_1 String,
    value_2 String
)
ENGINE = TinyLog;

INSERT INTO 02184_range_dictionary_source_table VALUES (1, 0, 18446744073709551615, 'value0', 'value1', 'value2');

DROP DICTIONARY IF EXISTS 02184_range_dictionary;
CREATE DICTIONARY 02184_range_dictionary
(
    id UInt64,
    start UInt64,
    end UInt64,
    value_0 String,
    value_1 String,
    value_2 String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02184_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT * FROM 02184_range_dictionary;
SELECT dictGet('02184_range_dictionary', ('value_0', 'value_1', 'value_2'), 1, 18446744073709551615);
SELECT dictHas('02184_range_dictionary', 1, 18446744073709551615);

DROP DICTIONARY 02184_range_dictionary;
DROP TABLE 02184_range_dictionary_source_table;
