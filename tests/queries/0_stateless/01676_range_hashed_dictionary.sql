CREATE TABLE date_table
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO date_table VALUES(1, toDate('2019-05-05'), toDate('2019-05-20'), 0.33);
INSERT INTO date_table VALUES(1, toDate('2019-05-21'), toDate('2019-05-30'), 0.42);
INSERT INTO date_table VALUES(2, toDate('2019-05-21'), toDate('2019-05-30'), 0.46);

CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB currentDatabase()))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate)
SETTINGS(dictionary_use_async_executor=1, max_threads=8)
;

SELECT 'Dictionary not nullable';
SELECT 'dictGet';
SELECT dictGet('range_dictionary', 'Tax', toUInt64(1), toDate('2019-05-15'));
SELECT dictGet('range_dictionary', 'Tax', toUInt64(1), toDate('2019-05-29'));
SELECT dictGet('range_dictionary', 'Tax', toUInt64(2), toDate('2019-05-29'));
SELECT dictGet('range_dictionary', 'Tax', toUInt64(2), toDate('2019-05-31'));
SELECT dictGetOrDefault('range_dictionary', 'Tax', toUInt64(2), toDate('2019-05-31'), 0.4);
SELECT 'dictHas';
SELECT dictHas('range_dictionary', toUInt64(1), toDate('2019-05-15'));
SELECT dictHas('range_dictionary', toUInt64(1), toDate('2019-05-29'));
SELECT dictHas('range_dictionary', toUInt64(2), toDate('2019-05-29'));
SELECT dictHas('range_dictionary', toUInt64(2), toDate('2019-05-31'));
SELECT 'select columns from dictionary';
SELECT 'allColumns';
SELECT * FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;
SELECT 'noColumns';
SELECT 1 FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumns';
SELECT CountryID, StartDate, Tax FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumn';
SELECT Tax FROM range_dictionary ORDER BY CountryID, StartDate, EndDate;

DROP DICTIONARY range_dictionary;
DROP TABLE date_table;

CREATE TABLE date_table
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO date_table VALUES(1, toDate('2019-05-05'), toDate('2019-05-20'), 0.33);
INSERT INTO date_table VALUES(1, toDate('2019-05-21'), toDate('2019-05-30'), 0.42);
INSERT INTO date_table VALUES(2, toDate('2019-05-21'), toDate('2019-05-30'), NULL);

CREATE DICTIONARY range_dictionary_nullable
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Nullable(Float64) DEFAULT 0.2
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB currentDatabase()))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT 'Dictionary nullable';
SELECT 'dictGet';
SELECT dictGet('range_dictionary_nullable', 'Tax', toUInt64(1), toDate('2019-05-15'));
SELECT dictGet('range_dictionary_nullable', 'Tax', toUInt64(1), toDate('2019-05-29'));
SELECT dictGet('range_dictionary_nullable', 'Tax', toUInt64(2), toDate('2019-05-29'));
SELECT dictGet('range_dictionary_nullable', 'Tax', toUInt64(2), toDate('2019-05-31'));
SELECT dictGetOrDefault('range_dictionary_nullable', 'Tax', toUInt64(2), toDate('2019-05-31'), 0.4);
SELECT 'dictHas';
SELECT dictHas('range_dictionary_nullable', toUInt64(1), toDate('2019-05-15'));
SELECT dictHas('range_dictionary_nullable', toUInt64(1), toDate('2019-05-29'));
SELECT dictHas('range_dictionary_nullable', toUInt64(2), toDate('2019-05-29'));
SELECT dictHas('range_dictionary_nullable', toUInt64(2), toDate('2019-05-31'));
SELECT 'select columns from dictionary';
SELECT 'allColumns';
SELECT * FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;
SELECT 'noColumns';
SELECT 1 FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumns';
SELECT CountryID, StartDate, Tax FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;
SELECT 'onlySpecificColumn';
SELECT Tax FROM range_dictionary_nullable ORDER BY CountryID, StartDate, EndDate;

DROP DICTIONARY range_dictionary_nullable;
DROP TABLE date_table;

