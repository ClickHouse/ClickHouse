-- Tags: no-fasttest

SET send_logs_level = 'fatal';
SET check_table_dependencies=0;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_for_dict
(
  key_column UInt64,
  second_column UInt8,
  third_column String,
  fourth_column Float64
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_for_dict SELECT number, number % 17, toString(number * number), number / 2.0 from numbers(100);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq',
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(11));
SELECT second_column FROM {CLICKHOUSE_DATABASE:Identifier}.dict1 WHERE key_column = 11;
SELECT dictGetString({CLICKHOUSE_DATABASE:String} || '.dict1', 'third_column', toUInt64(12));
SELECT third_column FROM {CLICKHOUSE_DATABASE:Identifier}.dict1 WHERE key_column = 12;
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict1', 'fourth_column', toUInt64(14));
SELECT fourth_column FROM {CLICKHOUSE_DATABASE:Identifier}.dict1 WHERE key_column = 14;

SELECT count(distinct(dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(number)))) from numbers(100);

DETACH DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1;

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(11)); -- {serverError BAD_ARGUMENTS}

ATTACH DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1;

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(11));

DROP DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1;

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(11)); -- {serverError BAD_ARGUMENTS}

-- SOURCE(CLICKHOUSE(...)) uses default params if not specified
DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.dict1;

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq',
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(11));

SELECT count(distinct(dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', toUInt64(number)))) from numbers(100);

DROP DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1;

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq',
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column, third_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(MIN 1 MAX 10)
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1));

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', tuple(toUInt64(11), '121'));
SELECT dictGetFloat64({CLICKHOUSE_DATABASE:String} || '.dict1', 'fourth_column', tuple(toUInt64(14), '196'));

DETACH DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1;

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', tuple(toUInt64(11), '121')); -- {serverError BAD_ARGUMENTS}

ATTACH DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict1;

SELECT dictGetUInt8({CLICKHOUSE_DATABASE:String} || '.dict1', 'second_column', tuple(toUInt64(11), '121'));

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict2
(
  key_column UInt64 DEFAULT 0,
  some_column String EXPRESSION toString(fourth_column),
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(MIN 1 MAX 10)
LAYOUT(HASHED());

SELECT dictGetString({CLICKHOUSE_DATABASE:String} || '.dict2', 'some_column', toUInt64(12));

-- NOTE: database = currentDatabase() is not mandatory
SELECT name, engine FROM system.tables WHERE database = {CLICKHOUSE_DATABASE:String} ORDER BY name;

SELECT database, name, type FROM system.dictionaries WHERE database = {CLICKHOUSE_DATABASE:String} ORDER BY name;

-- check dictionary will not update
CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict3
(
  key_column UInt64 DEFAULT 0,
  some_column String EXPRESSION toString(fourth_column),
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(0)
LAYOUT(HASHED());

SELECT dictGetString({CLICKHOUSE_DATABASE:String} || '.dict3', 'some_column', toUInt64(12));

-- dictGet with table name
USE {CLICKHOUSE_DATABASE:Identifier};
SELECT dictGetString(dict3, 'some_column', toUInt64(12));
SELECT dictGetString({CLICKHOUSE_DATABASE:Identifier}.dict3, 'some_column', toUInt64(12));
SELECT dictGetString(default.dict3, 'some_column', toUInt64(12)); -- {serverError BAD_ARGUMENTS}
SELECT dictGet(dict3, 'some_column', toUInt64(12));
SELECT dictGet({CLICKHOUSE_DATABASE:Identifier}.dict3, 'some_column', toUInt64(12));
SELECT dictGet(default.dict3, 'some_column', toUInt64(12)); -- {serverError BAD_ARGUMENTS}
USE default;

-- alias should be handled correctly
SELECT {CLICKHOUSE_DATABASE:String} || '.dict3' as n, dictGet(n, 'some_column', toUInt64(12));

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.table_for_dict;

SYSTEM RELOAD DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict3; -- {serverError UNKNOWN_TABLE}

SELECT dictGetString({CLICKHOUSE_DATABASE:String} || '.dict3', 'some_column', toUInt64(12));
