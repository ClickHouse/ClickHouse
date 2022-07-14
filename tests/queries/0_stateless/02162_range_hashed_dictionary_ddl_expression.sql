DROP TABLE IF EXISTS _02162_table;
CREATE TABLE _02162_table
(
    id UInt64,
    value String,
    range_value UInt64
) ENGINE=TinyLog;

INSERT INTO _02162_table VALUES (0, 'Value', 1);

DROP DICTIONARY IF EXISTS _02162_dictionary;
CREATE DICTIONARY _02162_dictionary
(
    id UInt64,
    value String,
    range_value UInt64,
    start UInt64 EXPRESSION range_value,
    end UInt64 EXPRESSION range_value
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02162_table'))
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT * FROM _02162_dictionary;

DROP DICTIONARY _02162_dictionary;
DROP TABLE _02162_table;
