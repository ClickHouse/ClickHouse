-- Tags: no-replicated-database

CREATE TABLE IF NOT EXISTS truncate_test_set(id UInt64) ENGINE = Set;
CREATE TABLE IF NOT EXISTS truncate_test_log(id UInt64) ENGINE = Log;
CREATE TABLE IF NOT EXISTS truncate_test_memory(id UInt64) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS truncate_test_tiny_log(id UInt64) ENGINE = TinyLog;
CREATE TABLE IF NOT EXISTS truncate_test_stripe_log(id UInt64) ENGINE = StripeLog;
CREATE TABLE IF NOT EXISTS truncate_test_merge_tree(p Date, k UInt64) ENGINE = MergeTree ORDER BY p;

SELECT '======Before Truncate======';
INSERT INTO truncate_test_set VALUES(0);
INSERT INTO truncate_test_log VALUES(1);
INSERT INTO truncate_test_memory VALUES(1);
INSERT INTO truncate_test_tiny_log VALUES(1);
INSERT INTO truncate_test_stripe_log VALUES(1);
INSERT INTO truncate_test_merge_tree VALUES('2000-01-01', 1);
SELECT * FROM system.numbers WHERE number NOT IN truncate_test_set LIMIT 1;
SELECT * FROM truncate_test_log;
SELECT * FROM truncate_test_memory;
SELECT * FROM truncate_test_tiny_log;
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;

SELECT '======Testing Truncate Without ALL Keyword======';
TRUNCATE TABLES FROM IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
SELECT * FROM system.numbers WHERE number NOT IN truncate_test_set LIMIT 1;
SELECT * FROM truncate_test_log;
SELECT * FROM truncate_test_memory;
SELECT * FROM truncate_test_tiny_log;
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;

SELECT '======Insert Values Again======';
INSERT INTO truncate_test_set VALUES(0);
INSERT INTO truncate_test_log VALUES(1);
INSERT INTO truncate_test_memory VALUES(1);
INSERT INTO truncate_test_tiny_log VALUES(1);
INSERT INTO truncate_test_stripe_log VALUES(1);
INSERT INTO truncate_test_merge_tree VALUES('2000-01-01', 1);

SELECT '======Truncate With LIKE Keyword======';
TRUNCATE TABLES FROM IF EXISTS {CLICKHOUSE_DATABASE:Identifier} LIKE '%merge_tree';
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;

SELECT '======Insert Values Again======';
INSERT INTO truncate_test_merge_tree VALUES('2000-01-01', 1);

SELECT '======Truncate With NOT LIKE Keyword======';
TRUNCATE TABLES FROM IF EXISTS {CLICKHOUSE_DATABASE:Identifier} NOT LIKE '%merge_tree';
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;

TRUNCATE TABLES FROM IF EXISTS {CLICKHOUSE_DATABASE:Identifier} NOT LIKE '%stripe%';
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;

SELECT '======After Truncate And Insert Data======';
INSERT INTO truncate_test_set VALUES(0);
INSERT INTO truncate_test_log VALUES(1);
INSERT INTO truncate_test_memory VALUES(1);
INSERT INTO truncate_test_tiny_log VALUES(1);
INSERT INTO truncate_test_stripe_log VALUES(1);
INSERT INTO truncate_test_merge_tree VALUES('2000-01-01', 1);
SELECT * FROM system.numbers WHERE number NOT IN truncate_test_set LIMIT 1;
SELECT * FROM truncate_test_log;
SELECT * FROM truncate_test_memory;
SELECT * FROM truncate_test_tiny_log;
SELECT * FROM truncate_test_stripe_log;
SELECT * FROM truncate_test_merge_tree;

DROP TABLE truncate_test_log;
DROP TABLE truncate_test_memory;
DROP TABLE truncate_test_tiny_log;
DROP TABLE truncate_test_stripe_log;
DROP TABLE truncate_test_merge_tree;
