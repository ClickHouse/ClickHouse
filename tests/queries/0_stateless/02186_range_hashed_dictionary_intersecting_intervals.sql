DROP TABLE IF EXISTS _02186_range_dictionary_source_table;
CREATE TABLE _02186_range_dictionary_source_table
(
    id UInt64,
    start Date,
    end Date,
    value String
)
Engine = TinyLog;

INSERT INTO _02186_range_dictionary_source_table VALUES (1, '2020-01-01', '2100-01-01', 'Value0');
INSERT INTO _02186_range_dictionary_source_table VALUES (1, '2020-01-02', '2100-01-01', 'Value1');
INSERT INTO _02186_range_dictionary_source_table VALUES (1, '2020-01-03', '2100-01-01', 'Value2');

SELECT 'Source table';
SELECT * FROM _02186_range_dictionary_source_table;

DROP DICTIONARY IF EXISTS _02186_range_dictionary;
CREATE DICTIONARY _02186_range_dictionary
(
    id UInt64,
    start Date,
    end Date,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02186_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'min'))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'Dictionary .range_lookup_strategy = min';

SELECT * FROM _02186_range_dictionary;

select dictGet('_02186_range_dictionary', 'value', toUInt64(1), toDate('2020-01-01'));
select dictGet('_02186_range_dictionary', 'value', toUInt64(1), toDate('2020-01-02'));
select dictGet('_02186_range_dictionary', 'value', toUInt64(1), toDate('2020-01-03'));

DROP DICTIONARY _02186_range_dictionary;

CREATE DICTIONARY _02186_range_dictionary
(
    id UInt64,
    start Date,
    end Date,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02186_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'Dictionary .range_lookup_strategy = max';

SELECT * FROM _02186_range_dictionary;

select dictGet('_02186_range_dictionary', 'value', toUInt64(1), toDate('2020-01-01'));
select dictGet('_02186_range_dictionary', 'value', toUInt64(1), toDate('2020-01-02'));
select dictGet('_02186_range_dictionary', 'value', toUInt64(1), toDate('2020-01-03'));

DROP DICTIONARY _02186_range_dictionary;
DROP TABLE _02186_range_dictionary_source_table;
