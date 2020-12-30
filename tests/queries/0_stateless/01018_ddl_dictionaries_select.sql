SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

CREATE TABLE database_for_dict.table_for_dict
(
  key_column UInt64,
  second_column UInt8,
  third_column String,
  fourth_column Float64
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict.table_for_dict SELECT number, number % 17, toString(number * number), number / 2.0 from numbers(100);

DROP DICTIONARY IF EXISTS database_for_dict.dict1;

CREATE DICTIONARY database_for_dict.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq',
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(11));
SELECT second_column FROM database_for_dict.dict1 WHERE key_column = 11;
SELECT dictGetString('database_for_dict.dict1', 'third_column', toUInt64(12));
SELECT third_column FROM database_for_dict.dict1 WHERE key_column = 12;
SELECT dictGetFloat64('database_for_dict.dict1', 'fourth_column', toUInt64(14));
SELECT fourth_column FROM database_for_dict.dict1 WHERE key_column = 14;

SELECT count(distinct(dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(number)))) from numbers(100);

DETACH DICTIONARY database_for_dict.dict1;

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(11)); -- {serverError 36}

ATTACH DICTIONARY database_for_dict.dict1;

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(11));

DROP DICTIONARY database_for_dict.dict1;

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(11)); -- {serverError 36}

-- SOURCE(CLICKHOUSE(...)) uses default params if not specified
DROP DICTIONARY IF EXISTS database_for_dict.dict1;

CREATE DICTIONARY database_for_dict.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq',
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(11));

SELECT count(distinct(dictGetUInt8('database_for_dict.dict1', 'second_column', toUInt64(number)))) from numbers(100);

DROP DICTIONARY database_for_dict.dict1;

CREATE DICTIONARY database_for_dict.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq',
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column, third_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1));

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', tuple(toUInt64(11), '121'));
SELECT dictGetFloat64('database_for_dict.dict1', 'fourth_column', tuple(toUInt64(14), '196'));

DETACH DICTIONARY database_for_dict.dict1;

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', tuple(toUInt64(11), '121')); -- {serverError 36}

ATTACH DICTIONARY database_for_dict.dict1;

SELECT dictGetUInt8('database_for_dict.dict1', 'second_column', tuple(toUInt64(11), '121'));

CREATE DICTIONARY database_for_dict.dict2
(
  key_column UInt64 DEFAULT 0,
  some_column String EXPRESSION toString(fourth_column),
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(HASHED());

SELECT dictGetString('database_for_dict.dict2', 'some_column', toUInt64(12));

SELECT name, engine FROM system.tables WHERE database = 'database_for_dict' ORDER BY name;

SELECT database, name, type FROM system.dictionaries WHERE database = 'database_for_dict' ORDER BY name;

-- check dictionary will not update
CREATE DICTIONARY database_for_dict.dict3
(
  key_column UInt64 DEFAULT 0,
  some_column String EXPRESSION toString(fourth_column),
  fourth_column Float64 DEFAULT 42.0
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT dictGetString('database_for_dict.dict3', 'some_column', toUInt64(12));

-- dictGet with table name
USE database_for_dict;
SELECT dictGetString(dict3, 'some_column', toUInt64(12));
SELECT dictGetString(database_for_dict.dict3, 'some_column', toUInt64(12));
SELECT dictGetString(default.dict3, 'some_column', toUInt64(12)); -- {serverError 36}
SELECT dictGet(dict3, 'some_column', toUInt64(12));
SELECT dictGet(database_for_dict.dict3, 'some_column', toUInt64(12));
SELECT dictGet(default.dict3, 'some_column', toUInt64(12)); -- {serverError 36}
USE default;

-- alias should be handled correctly
SELECT 'database_for_dict.dict3' as n, dictGet(n, 'some_column', toUInt64(12));

DROP TABLE database_for_dict.table_for_dict;

SYSTEM RELOAD DICTIONARIES; -- {serverError 60}

SELECT dictGetString('database_for_dict.dict3', 'some_column', toUInt64(12));

DROP DATABASE IF EXISTS database_for_dict;
