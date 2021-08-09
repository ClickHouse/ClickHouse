SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS database_for_dict_01018;

CREATE DATABASE database_for_dict_01018;

DROP TABLE IF EXISTS database_for_dict_01018.table_for_dict;

CREATE TABLE database_for_dict_01018.table_for_dict
(
  key_column UInt64,
  second_column UInt8,
  third_column String
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict_01018.table_for_dict VALUES (1, 100, 'Hello world');

DROP DATABASE IF EXISTS db_01018;

CREATE DATABASE db_01018;

SELECT '=DICTIONARY in Ordinary DB';

DROP DICTIONARY IF EXISTS db_01018.dict1;

CREATE DICTIONARY db_01018.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict_01018'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SHOW CREATE DICTIONARY db_01018.dict1;

SHOW DICTIONARIES FROM db_01018 LIKE 'dict1';

EXISTS DICTIONARY db_01018.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

SELECT '==DETACH DICTIONARY';
DETACH DICTIONARY db_01018.dict1;

SHOW DICTIONARIES FROM db_01018 LIKE 'dict1';

EXISTS DICTIONARY db_01018.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

SELECT '==ATTACH DICTIONARY';
ATTACH DICTIONARY db_01018.dict1;

SHOW DICTIONARIES FROM db_01018 LIKE 'dict1';

EXISTS DICTIONARY db_01018.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

SELECT '==DROP DICTIONARY';

DROP DICTIONARY IF EXISTS db_01018.dict1;

SHOW DICTIONARIES FROM db_01018 LIKE 'dict1';

EXISTS DICTIONARY db_01018.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

DROP DATABASE IF EXISTS db_01018;

DROP DATABASE IF EXISTS memory_db;

CREATE DATABASE memory_db ENGINE = Memory;

SELECT '=DICTIONARY in Memory DB';

CREATE DICTIONARY memory_db.dict2
(
  key_column UInt64 DEFAULT 0 INJECTIVE,
  second_column UInt8 DEFAULT 1 EXPRESSION rand() % 222,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict_01018'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SHOW CREATE DICTIONARY memory_db.dict2;

SHOW DICTIONARIES FROM memory_db LIKE 'dict2';

EXISTS DICTIONARY memory_db.dict2;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict2';

SELECT '=DICTIONARY in Lazy DB';

DROP DATABASE IF EXISTS lazy_db;

CREATE DATABASE lazy_db ENGINE = Lazy(1);

CREATE DICTIONARY lazy_db.dict3
(
  key_column UInt64 DEFAULT 0 INJECTIVE,
  second_column UInt8 DEFAULT 1 EXPRESSION rand() % 222,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column, second_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict_01018'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(COMPLEX_KEY_HASHED()); --{serverError 1}

DROP DATABASE IF EXISTS lazy_db;

SELECT '=DROP DATABASE WITH DICTIONARY';

DROP DATABASE IF EXISTS db_01018;

CREATE DATABASE db_01018;

CREATE DICTIONARY db_01018.dict4
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict_01018'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SHOW DICTIONARIES FROM db_01018;

DROP DATABASE IF EXISTS db_01018;

CREATE DATABASE db_01018;

SHOW DICTIONARIES FROM db_01018;

CREATE DICTIONARY db_01018.dict4
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict_01018'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SHOW DICTIONARIES FROM db_01018;

DROP DATABASE IF EXISTS db_01018;

DROP TABLE IF EXISTS database_for_dict_01018.table_for_dict;

DROP DATABASE IF EXISTS database_for_dict_01018;
DROP DATABASE IF EXISTS memory_db;
