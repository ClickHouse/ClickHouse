DROP TABLE IF EXISTS _02179_test_table;
CREATE TABLE _02179_test_table
(
    id UInt64,
    value String,
    start Int64,
    end Int64
) Engine = TinyLog;

INSERT INTO _02179_test_table VALUES (0, 'Value', 10, 0);
INSERT INTO _02179_test_table VALUES (0, 'Value', 15, 10);
INSERT INTO _02179_test_table VALUES (0, 'Value', 15, 20);

DROP DICTIONARY IF EXISTS _02179_test_dictionary;
CREATE DICTIONARY _02179_test_dictionary
(
    id UInt64,
    value String DEFAULT 'DefaultValue',
    start Int64,
    end Int64
) PRIMARY KEY id
LAYOUT(RANGE_HASHED())
SOURCE(CLICKHOUSE(TABLE '_02179_test_table'))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT dictGet('_02179_test_dictionary', 'value', 0, 15);
SELECT dictGet('_02179_test_dictionary', 'value', 0, 5);

SELECT dictHas('_02179_test_dictionary', 0, 15);
SELECT dictHas('_02179_test_dictionary', 0, 5);

SELECT * FROM _02179_test_dictionary;

DROP DICTIONARY _02179_test_dictionary;
DROP TABLE _02179_test_table;
