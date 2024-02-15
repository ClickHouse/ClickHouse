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

SELECT * FROM system.warnings where message in ('The number of attached tables is more than 5', 'The number of attached databases is more than 2', 'The number of active parts is more than 10');

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
