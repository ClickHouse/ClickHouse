-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that the key of the query cache is not only formed by the query AST but also by
-- (1) the current database (`USE db`, issue #64136),
-- (2) the query settings


SELECT 'Test (1)';

SYSTEM DROP QUERY CACHE;

DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS db2;

CREATE DATABASE db1;
CREATE DATABASE db2;

CREATE TABLE db1.tab(a UInt64, PRIMARY KEY a);
CREATE TABLE db2.tab(a UInt64, PRIMARY KEY a);

INSERT INTO db1.tab values(1);
INSERT INTO db2.tab values(2);

USE db1;
SELECT * FROM tab SETTINGS use_query_cache = 1;

USE db2;
SELECT * FROM tab SETTINGS use_query_cache = 1;

DROP DATABASE db1;
DROP DATABASE db2;

SYSTEM DROP QUERY CACHE;


SELECT 'Test (2)';

-- test with query-level settings
SELECT 1 SETTINGS use_query_cache = 1, limit = 1, use_skip_indexes = 0 Format Null;
SELECT 1 SETTINGS use_query_cache = 1, use_skip_indexes = 0 Format Null;
SELECT 1 SETTINGS use_query_cache = 1, use_skip_indexes = 1 Format Null;
SELECT 1 SETTINGS use_query_cache = 1, max_block_size = 1 Format Null;

-- 4x the same query but with different settings each. There should yield four entries in the query cache.
SELECT count(query) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

-- test with mixed session-level/query-level settings
SET use_query_cache = 1;
SET limit = 1;
SELECT 1 SETTINGS use_skip_indexes = 0 Format Null;
SET limit = default;
SET use_skip_indexes = 0;
SELECT 1 Format Null;
SET use_skip_indexes = 1;
SELECT 1 SETTINGS use_skip_indexes = 1 Format Null;
SET use_skip_indexes = default;
SET max_block_size = 1;
SELECT 1 Format Null;
SET max_block_size = default;

SET use_query_cache = default;

-- 4x the same query but with different settings each. There should yield four entries in the query cache.
SELECT count(query) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

