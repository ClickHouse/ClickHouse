SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

SELECT '***date dict***';

CREATE TABLE database_for_dict.date_table
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO database_for_dict.date_table VALUES(1, toDate('2019-05-05'), toDate('2019-05-20'), 0.33);
INSERT INTO database_for_dict.date_table VALUES(1, toDate('2019-05-21'), toDate('2019-05-30'), 0.42);
INSERT INTO database_for_dict.date_table VALUES(2, toDate('2019-05-21'), toDate('2019-05-30'), 0.46);

CREATE DICTIONARY database_for_dict.dict1
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Float64
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', toUInt64(1), toDate('2019-05-15'));
SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', toUInt64(1), toDate('2019-05-29'));
SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', toUInt64(2), toDate('2019-05-29'));
SELECT dictGetFloat64('database_for_dict.dict1', 'Tax', toUInt64(2), toDate('2019-05-31'));

SELECT '***datetime dict***';

CREATE TABLE database_for_dict.datetime_table
(
  CountryID UInt64,
  StartDate DateTime,
  EndDate DateTime,
  Tax Float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO database_for_dict.datetime_table VALUES(1, toDateTime('2019-05-05 00:00:00'), toDateTime('2019-05-20 00:00:00'), 0.33);
INSERT INTO database_for_dict.datetime_table VALUES(1, toDateTime('2019-05-21 00:00:00'), toDateTime('2019-05-30 00:00:00'), 0.42);
INSERT INTO database_for_dict.datetime_table VALUES(2, toDateTime('2019-05-21 00:00:00'), toDateTime('2019-05-30 00:00:00'), 0.46);

CREATE DICTIONARY database_for_dict.dict2
(
  CountryID UInt64,
  StartDate DateTime,
  EndDate DateTime,
  Tax Float64
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'datetime_table' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', toUInt64(1), toDateTime('2019-05-15 00:00:00'));
SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', toUInt64(1), toDateTime('2019-05-29 00:00:00'));
SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', toUInt64(2), toDateTime('2019-05-29 00:00:00'));
SELECT dictGetFloat64('database_for_dict.dict2', 'Tax', toUInt64(2), toDateTime('2019-05-31 00:00:00'));

SELECT '***hierarchy dict***';

CREATE TABLE database_for_dict.table_with_hierarchy
(
  RegionID UInt64,
  ParentRegionID UInt64,
  RegionName String
)
ENGINE = MergeTree()
ORDER BY RegionID;

INSERT INTO database_for_dict.table_with_hierarchy VALUES (3, 2, 'Hamovniki'), (2, 1, 'Moscow'), (1, 10000, 'Russia') (7, 10000, 'Ulan-Ude');


CREATE DICTIONARY database_for_dict.dictionary_with_hierarchy
(
    RegionID UInt64,
    ParentRegionID UInt64 HIERARCHICAL,
    RegionName String
)
PRIMARY KEY RegionID
SOURCE(CLICKHOUSE(host 'localhost' port tcpPort() user 'default' db 'database_for_dict' table 'table_with_hierarchy'))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1000);

SELECT dictGetString('database_for_dict.dictionary_with_hierarchy', 'RegionName', toUInt64(2));
SELECT dictGetHierarchy('database_for_dict.dictionary_with_hierarchy', toUInt64(3));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(3), toUInt64(2));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(7), toUInt64(10000));
SELECT dictIsIn('database_for_dict.dictionary_with_hierarchy', toUInt64(1), toUInt64(5));

DROP DATABASE IF EXISTS database_for_dict;
