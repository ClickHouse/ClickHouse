-- Tags: no-fasttest

SET send_logs_level = 'fatal';

SELECT '***date dict***';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.date_table
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.date_table VALUES(1, toDate('2019-05-05'), toDate('2019-05-20'), 0.33);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.date_table VALUES(1, toDate('2019-05-21'), toDate('2019-05-30'), 0.42);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.date_table VALUES(2, toDate('2019-05-21'), toDate('2019-05-30'), 0.46);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1
(
  CountryID UInt64,
  StartDate Date,
  EndDate Date,
  Tax Float64
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'date_table' DB currentDatabase()))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict1', 'Tax', toUInt64(1), toDate('2019-05-15'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict1', 'Tax', toUInt64(1), toDate('2019-05-29'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict1', 'Tax', toUInt64(2), toDate('2019-05-29'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict1', 'Tax', toUInt64(2), toDate('2019-05-31'));

SELECT '***datetime dict***';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.datetime_table
(
  CountryID UInt64,
  StartDate DateTime,
  EndDate DateTime,
  Tax Float64
)
ENGINE = MergeTree()
ORDER BY CountryID;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.datetime_table VALUES(1, toDateTime('2019-05-05 00:00:00'), toDateTime('2019-05-20 00:00:00'), 0.33);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.datetime_table VALUES(1, toDateTime('2019-05-21 00:00:00'), toDateTime('2019-05-30 00:00:00'), 0.42);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.datetime_table VALUES(2, toDateTime('2019-05-21 00:00:00'), toDateTime('2019-05-30 00:00:00'), 0.46);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict2
(
  CountryID UInt64,
  StartDate DateTime,
  EndDate DateTime,
  Tax Float64
)
PRIMARY KEY CountryID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'datetime_table' DB currentDatabase()))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);

SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict2', 'Tax', toUInt64(1), toDateTime('2019-05-15 00:00:00'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict2', 'Tax', toUInt64(1), toDateTime('2019-05-29 00:00:00'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict2', 'Tax', toUInt64(2), toDateTime('2019-05-29 00:00:00'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict2', 'Tax', toUInt64(2), toDateTime('2019-05-31 00:00:00'));

SELECT '***hierarchy dict***';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_with_hierarchy
(
  RegionID UInt64,
  ParentRegionID UInt64,
  RegionName String
)
ENGINE = MergeTree()
ORDER BY RegionID;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_with_hierarchy VALUES (3, 2, 'Hamovniki'), (2, 1, 'Moscow'), (1, 10000, 'Russia') (7, 10000, 'Ulan-Ude');


CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dictionary_with_hierarchy
(
    RegionID UInt64,
    ParentRegionID UInt64 HIERARCHICAL,
    RegionName String
)
PRIMARY KEY RegionID
SOURCE(CLICKHOUSE(host 'localhost' port tcpPort() user 'default' db currentDatabase() table 'table_with_hierarchy'))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1000);

SELECT dictGetString({CLICKHOUSE_DATABASE:String} || '.dictionary_with_hierarchy', 'RegionName', toUInt64(2));
SELECT dictGetHierarchy({CLICKHOUSE_DATABASE:String} || '.dictionary_with_hierarchy', toUInt64(3));
SELECT dictIsIn({CLICKHOUSE_DATABASE:String} || '.dictionary_with_hierarchy', toUInt64(3), toUInt64(2));
SELECT dictIsIn({CLICKHOUSE_DATABASE:String} || '.dictionary_with_hierarchy', toUInt64(7), toUInt64(10000));
SELECT dictIsIn({CLICKHOUSE_DATABASE:String} || '.dictionary_with_hierarchy', toUInt64(1), toUInt64(5));

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
