CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.table_for_dict;
DROP TABLE IF EXISTS test.table;
DROP DICTIONARY IF EXISTS test.dict1;

-- It used for loading in dictionaries via ClickHouse source
CREATE TABLE test.table_for_dict
 (
    first_column UInt8,
    second_column UInt8,
    third_column UInt8
 )
ENGINE = TinyLog;

INSERT INTO TABLE test.table_for_dict VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333);

-- Check that dictionary name doesn't intersect with tables names
CREATE DICTIONARY test.table_for_dict -- { serverError 57 }
 (
    second UInt8 DEFAULT 1
 )
PRIMARY KEY first_column
SOURCE(FILE(PATH '', FORMAT 'TabSeparated'))
LAYOUT(FLAT())
LIFETIME(MIN 1, MAX 10);

CREATE DICTIONARY test.dict1
 (
    second_column UInt8 DEFAULT 1,
    third_column UInt8 DEFAULT 1
 )
PRIMARY KEY first_column
SOURCE(CLICKHOUSE(HOST 'localhost', PORT '9000', USER 'default', PASSWORD '', DB 'test', TABLE 'table_for_dict'))
LAYOUT(FLAT())
LIFETIME(MIN 1, MAX 10);

-- Can't create dictionary if other dictionary with the same name already exists.
CREATE DICTIONARY test.dict1 -- { serverError 446 }
 (
    second UInt8 DEFAULT 1
 )
PRIMARY KEY first_column
SOURCE(CLICKHOUSE(HOST 'localhost', PORT '9000', USER 'default', PASSWORD '', DB 'test', TABLE 'table_for_dict'))
LAYOUT(FLAT())
LIFETIME(MIN 1, MAX 10);

SHOW CREATE DICTIONARY test.dict1;
SHOW CREATE test.dict1; -- { serverError 107 }

SELECT
    name, database, definition_type,
    origin, type, key,
    attribute.names, attribute.types,
    query_count, hit_rate, element_count,
    load_factor, source, last_exception
FROM system.dictionaries WHERE database = 'test';

CREATE TABLE test.table
 (
    first_column UInt64,
    second_column UInt8,
    third_column UInt8
 )
ENGINE = Dictionary(test.dict1);

SHOW CREATE test.table;
SELECT * FROM test.table; -- Here might be a race because of asynchronous loading of values in the dictionary

-- TODO(s-mx): add here folowing checks:
-- 1) COMPOSITE KEY and work with that
-- 2) dict functions
-- 3) RANGE in definition of a dictionary
-- 4)

DROP TABLE test.table_for_dict;
DROP TABLE test.table;
DROP DICTIONARY test.dict1;
