-- Tags: no-shared-merge-tree, no-parallel, no-release
-- Tag no-parallel: uses shared cache state and must remain isolated from concurrent cache tests.
-- Tag no-release: reads `table_uuid` from `system.query_condition_cache`, which is available only
-- in debug and sanitizer builds.
-- no-shared-merge-tree: doesn't support databases without UUID

-- Testcase for https://github.com/ClickHouse/ClickHouse/issues/92863
-- Tables/parts without UUID should not enter into the query condition cache.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;

USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE tab
(
    id Int32,
    val Int32
) Engine = MergeTree ORDER BY id
SETTINGS index_granularity = 8;

INSERT INTO tab SELECT number, number * 8 FROM numbers(100);

-- Prints 00000000-0000-0000-0000-000000000000.
SELECT uuid FROM system.parts WHERE database = currentDatabase();

SET use_query_condition_cache = 1;

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count(*) from system.query_condition_cache WHERE table_uuid IN (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name IN ('tab')); -- no entry

SELECT count(*) FROM tab WHERE val = 24; -- 1 match

SELECT count(*) from system.query_condition_cache WHERE table_uuid IN (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name IN ('tab')); -- still no entry

DROP TABLE tab;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
