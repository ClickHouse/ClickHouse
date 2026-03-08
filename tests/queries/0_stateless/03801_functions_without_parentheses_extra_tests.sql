-- Tags: no-parallel

-- Test SQL standard niladic functions without parentheses
SET enable_analyzer = 1;

-- Test all niladic functions
SELECT DATABASE = currentDatabase();
SELECT SCHEMA = currentDatabase();
SELECT USER = currentUser();
SELECT CURRENT_USER = currentUser();
SELECT toTypeName(CURDATE);

-- Test the Default Path resolution
CREATE TABLE t_niladic_default (ts DateTime DEFAULT CURRENT_TIMESTAMP, x UInt8) ENGINE = Memory;
INSERT INTO t_niladic_default (x) VALUES (1);
SELECT toTypeName(ts), ts > '2020-01-01' FROM t_niladic_default;
DROP TABLE t_niladic_default;

-- Test insert select path
CREATE TABLE t_niladic_insert (ts DateTime) ENGINE = Memory;
INSERT INTO t_niladic_insert SELECT CURRENT_TIMESTAMP;
SELECT count() FROM t_niladic_insert WHERE ts > '2020-01-01';
DROP TABLE t_niladic_insert;