-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_02931;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_1 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_2 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_3 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_4 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_5 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_6 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_7 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_8 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_9 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_10 (id Int32, str String) Engine=Memory;
CREATE TABLE IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_11 (id Int32, str String) Engine=Memory;

CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_1 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_1;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_2 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_2;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_3 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_3;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_4 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_4;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_5 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_5;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_6 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_6;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_7 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_7;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_8 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_8;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_9 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_9;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_10 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_10;
CREATE VIEW IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_view_11 AS SELECT * FROM test_max_num_to_warn_02931.test_max_num_to_warn_11;

CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_1 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_1'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_2 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_2'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_3 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_3'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_4 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_4'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_5 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_5'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_6 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_6'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_7 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_7'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_8 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_8'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_9 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_9'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE DICTIONARY IF NOT EXISTS test_max_num_to_warn_02931.test_max_num_to_warn_dict_10 (id Int32, str String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB 'test_max_num_to_warn_02931' TABLE 'test_max_num_to_warn_10'))LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);

CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_1;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_2;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_3;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_4;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_5;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_6;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_7;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_8;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_9;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_10;
CREATE DATABASE IF NOT EXISTS test_max_num_to_warn_11;

INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_1 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_2 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_3 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_4 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_5 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_6 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_7 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_8 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_9 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_10 VALUES (1, 'Hello');
INSERT INTO test_max_num_to_warn_02931.test_max_num_to_warn_11 VALUES (1, 'Hello');

SELECT * FROM system.warnings where message in (
    'The number of attached tables is more than 5.',
    'The number of attached views is more than 5.',
    'The number of attached dictionaries is more than 5.',
    'The number of attached databases is more than 2.',
    'The number of active parts is more than 10.'
);

DROP DATABASE IF EXISTS test_max_num_to_warn_02931;
DROP DATABASE IF EXISTS test_max_num_to_warn_1;
DROP DATABASE IF EXISTS test_max_num_to_warn_2;
DROP DATABASE IF EXISTS test_max_num_to_warn_3;
DROP DATABASE IF EXISTS test_max_num_to_warn_4;
DROP DATABASE IF EXISTS test_max_num_to_warn_5;
DROP DATABASE IF EXISTS test_max_num_to_warn_6;
DROP DATABASE IF EXISTS test_max_num_to_warn_7;
DROP DATABASE IF EXISTS test_max_num_to_warn_8;
DROP DATABASE IF EXISTS test_max_num_to_warn_9;
DROP DATABASE IF EXISTS test_max_num_to_warn_10;
DROP DATABASE IF EXISTS test_max_num_to_warn_11;
