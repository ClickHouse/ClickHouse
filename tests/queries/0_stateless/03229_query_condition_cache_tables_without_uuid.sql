-- Tags: no-parallel, no-shared-merge-tree
-- no-parallel: creates own database
-- no-shared-merge-tree: doesn't support databases without UUID

-- Testcase for https://github.com/ClickHouse/ClickHouse/issues/92863
-- Tables/parts without UUID should not enter into the query condition cache.

DROP DATABASE IF EXISTS memory_db;

CREATE DATABASE memory_db ENGINE = Memory;

USE memory_db;

CREATE TABLE tab
(
    id Int32,
    val Int32
) Engine = MergeTree ORDER BY id
SETTINGS index_granularity = 8;

INSERT INTO tab SELECT number, number * 8 FROM numbers(100);

-- Prints 00000000-0000-0000-0000-000000000000.
SELECT uuid FROM system.parts WHERE database = 'memory_db';

SET use_query_condition_cache = 1;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count(*) from system.query_condition_cache; -- no entry

SELECT count(*) FROM tab WHERE val = 24; -- 1 match

SELECT count(*) from system.query_condition_cache; -- still no entry

DROP TABLE tab;

DROP DATABASE memory_db;
