-- Tags: no-parallel

DROP DATABASE IF EXISTS test_dict_db;
DROP DATABASE IF EXISTS test_normal_db;

CREATE DATABASE test_dict_db;

CREATE OR REPLACE DICTIONARY test_dict_db.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS id, toString(number) AS value FROM numbers(10)'))
LAYOUT(FLAT())
LIFETIME(0);

SYSTEM RELOAD DICTIONARY test_dict_db.test_dictionary;

CREATE DATABASE test_normal_db;
USE test_normal_db;

CREATE OR REPLACE DICTIONARY test_normal_db.local_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS id, toString(number) AS value FROM numbers(5)'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT 'Test 1: Without setting (should fail)';
SELECT dictGet('test_dictionary', 'value', toUInt64(1)); -- { serverError 36 }

SELECT 'Test 2: With explicit database';
SELECT dictHas('test_dict_db.test_dictionary', toUInt64(1));

SET default_dictionary_database = 'test_dict_db';

SELECT 'Test 3: Local dictionary in current database';
SELECT dictHas('local_dictionary', toUInt64(1));


SELECT 'Test 4: With default_dictionary_database setting';
SELECT dictHas('test_dictionary', toUInt64(1));

CREATE OR REPLACE DICTIONARY test_normal_db.test_dictionary
(
    id UInt64,
    value String DEFAULT 'local'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT number AS id, toString(number) AS value FROM numbers(5)'))
LAYOUT(HASHED())
LIFETIME(0);

USE default;
SELECT 'Test 5: Priority test';
SELECT dictHas('test_dictionary', toUInt64(6));

DROP DICTIONARY IF EXISTS test_dict_db.test_dictionary;
DROP DICTIONARY IF EXISTS test_normal_db.test_dictionary;
DROP DICTIONARY IF EXISTS test_normal_db.local_dictionary;
DROP DATABASE IF EXISTS test_dict_db;
DROP DATABASE IF EXISTS test_normal_db;
