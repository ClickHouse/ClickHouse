SET send_logs_level = 'none';

DROP DATABASE IF EXISTS ordinary_db;

CREATE DATABASE ordinary_db ENGINE = Ordinary;

SELECT '=DICTIONARY in Ordinary DB';

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

CREATE DICTIONARY ordinary_db.dict1
(
    key_column UInt64 DEFAULT 0,
    second_column UInt8 DEFAULT 1,
    third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD ''))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SHOW CREATE DICTIONARY ordinary_db.dict1;

SHOW DICTIONARIES FROM ordinary_db LIKE 'dict1';

EXISTS DICTIONARY ordinary_db.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

SELECT '==DETACH DICTIONARY';
DETACH DICTIONARY ordinary_db.dict1;

SHOW DICTIONARIES FROM ordinary_db LIKE 'dict1';

EXISTS DICTIONARY ordinary_db.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

SELECT '==ATTACH DICTIONARY';
ATTACH DICTIONARY ordinary_db.dict1;

SHOW DICTIONARIES FROM ordinary_db LIKE 'dict1';

EXISTS DICTIONARY ordinary_db.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

SELECT '==DROP DICTIONARY';

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

SHOW DICTIONARIES FROM ordinary_db LIKE 'dict1';

EXISTS DICTIONARY ordinary_db.dict1;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict1';

DROP DATABASE IF EXISTS ordinary_db;

DROP DATABASE IF EXISTS memory_db;

CREATE DATABASE memory_db ENGINE = Memory;

SELECT '=DICTIONARY in Memory DB';

CREATE DICTIONARY memory_db.dict2
(
  key_column UInt64 DEFAULT 0 INJECTIVE HIERARCHICAL,
  second_column UInt8 DEFAULT 1 EXPRESSION rand() % 222,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD ''))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SHOW CREATE DICTIONARY memory_db.dict2; -- {serverError 500}

SHOW DICTIONARIES FROM memory_db LIKE 'dict2';

EXISTS DICTIONARY memory_db.dict2;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict2';

SELECT '==DETACH DICTIONARY';
DETACH DICTIONARY memory_db.dict2;

SHOW DICTIONARIES FROM memory_db LIKE 'dict2';

EXISTS DICTIONARY memory_db.dict2;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict2';

SELECT '==ATTACH DICTIONARY';

ATTACH DICTIONARY memory_db.dict2;

SHOW DICTIONARIES FROM memory_db LIKE 'dict2';

EXISTS DICTIONARY memory_db.dict2;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict2';

SELECT '==DROP DICTIONARY';

DROP DICTIONARY IF EXISTS memory_db.dict2;

SHOW DICTIONARIES FROM memory_db LIKE 'dict2';

EXISTS DICTIONARY memory_db.dict2;

SELECT database, name FROM system.dictionaries WHERE name LIKE 'dict2';

DROP DATABASE IF EXISTS memory_db;

DROP DATABASE IF EXISTS dictionary_db;

CREATE DATABASE dictionary_db ENGINE = Dictionary;

CREATE DICTIONARY dictionary_db.dict2
(
    key_column UInt64 DEFAULT 0 INJECTIVE HIERARCHICAL,
    second_column UInt8 DEFAULT 1 EXPRESSION rand() % 222,
    third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column, second_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD ''))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()); -- {serverError 1}

DROP DATABASE IF EXISTS dictionary_db;
