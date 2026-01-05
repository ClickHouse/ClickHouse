-- Testcase for https://github.com/ClickHouse/ClickHouse/issues/92863
-- Tables/parts without UUID should not enter into the query condition cache.

DROP DATABASE IF EXISTS memory_database;

CREATE DATABASE memory_database ENGINE = Memory;

USE memory_database;

CREATE TABLE table1
(
    id Int32,
    val Int32
) Engine = MergeTree ORDER BY id
SETTINGS index_granularity = 8;

INSERT INTO table1 SELECT number, number * 5 FROM numbers(100);

CREATE TABLE table2
(
    id Int32,
    val Int32
) Engine = MergeTree ORDER BY id
SETTINGS index_granularity = 8;

INSERT INTO table2 SELECT number, number * 8 FROM numbers(100);

-- Prints 00000000-0000-0000-0000-000000000000.
SELECT uuid FROM system.parts WHERE database = 'memory_database';

SET use_query_condition_cache = 1;

-- 0 rows
SELECT COUNT(*) FROM table1 WHERE val = 24;

-- 1 row
SELECT COUNT(*) FROM table2 WHERE val = 24;

DROP TABLE table1;

DROP TABLE table2;

DROP DATABASE memory_database;
