-- Tags: no-parallel

set allow_deprecated_syntax_for_merge_tree=1;

DROP DATABASE IF EXISTS truncate_test;
DROP TABLE IF EXISTS truncate_test_log;
DROP TABLE IF EXISTS truncate_test_memory;
DROP TABLE IF EXISTS truncate_test_tiny_log;
DROP TABLE IF EXISTS truncate_test_stripe_log;
DROP TABLE IF EXISTS truncate_test_merge_tree;
DROP TABLE IF EXISTS truncate_test_materialized_view;
DROP TABLE IF EXISTS truncate_test_materialized_depend;

CREATE DATABASE truncate_test;
CREATE TABLE truncate_test_set(id UInt64) ENGINE = Set;
CREATE TABLE truncate_test_log(id UInt64) ENGINE = Log;
CREATE TABLE truncate_test_memory(id UInt64) ENGINE = Memory;
CREATE TABLE truncate_test_tiny_log(id UInt64) ENGINE = TinyLog;
CREATE TABLE truncate_test_stripe_log(id UInt64) ENGINE = StripeLog;
CREATE TABLE truncate_test_merge_tree(p Date, k UInt64) ENGINE = MergeTree(p, k, 1);
CREATE TABLE truncate_test_materialized_depend(p Date, k UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW truncate_test_materialized_view ENGINE = MergeTree(p, k, 1) AS SELECT * FROM truncate_test_materialized_depend;

SELECT '======Before Truncate======';
INSERT INTO truncate_test_set VALUES(0);
INSERT INTO truncate_test_log VALUES(1);
INSERT INTO truncate_test_memory VALUES(1);
INSERT INTO truncate_test_tiny_log VALUES(1);
INSERT INTO truncate_test_stripe_log VALUES(1);
INSERT INTO truncate_test_merge_tree VALUES('2000-01-01', 1);
INSERT INTO truncate_test_materialized_depend VALUES('2000-01-01', 1);
SELECT * FROM system.numbers WHERE number NOT IN truncate_test_set LIMIT 1;
SELECT * FROM truncate_test_log;
SELECT * FROM truncate_test_memory;
SELECT * FROM truncate_test_tiny_log;
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;
SELECT * FROM truncate_test_materialized_view;

SELECT '======After Truncate And Empty======';
TRUNCATE TABLE truncate_test_set;
TRUNCATE TABLE truncate_test_log;
TRUNCATE TABLE truncate_test_memory;
TRUNCATE TABLE truncate_test_tiny_log;
TRUNCATE TABLE truncate_test_stripe_log;
TRUNCATE TABLE truncate_test_merge_tree;
TRUNCATE TABLE truncate_test_materialized_view;
SELECT * FROM system.numbers WHERE number NOT IN truncate_test_set LIMIT 1;
SELECT * FROM truncate_test_log;
SELECT * FROM truncate_test_memory;
SELECT * FROM truncate_test_tiny_log;
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;
SELECT * FROM truncate_test_materialized_view;

SELECT '======After Truncate And Insert Data======';
INSERT INTO truncate_test_set VALUES(0);
INSERT INTO truncate_test_log VALUES(1);
INSERT INTO truncate_test_memory VALUES(1);
INSERT INTO truncate_test_tiny_log VALUES(1);
INSERT INTO truncate_test_stripe_log VALUES(1);
INSERT INTO truncate_test_merge_tree VALUES('2000-01-01', 1);
INSERT INTO truncate_test_materialized_depend VALUES('2000-01-01', 1);
SELECT * FROM system.numbers WHERE number NOT IN truncate_test_set LIMIT 1;
SELECT * FROM truncate_test_log;
SELECT * FROM truncate_test_memory;
SELECT * FROM truncate_test_tiny_log;
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;
SELECT * FROM truncate_test_materialized_view;

DROP TABLE IF EXISTS truncate_test_set;
DROP TABLE IF EXISTS truncate_test_log;
DROP TABLE IF EXISTS truncate_test_memory;
DROP TABLE IF EXISTS truncate_test_tiny_log;
DROP TABLE IF EXISTS truncate_test_stripe_log;
DROP TABLE IF EXISTS truncate_test_merge_tree;
DROP TABLE IF EXISTS truncate_test_materialized_view;
DROP TABLE IF EXISTS truncate_test_materialized_depend;
DROP DATABASE IF EXISTS truncate_test;
