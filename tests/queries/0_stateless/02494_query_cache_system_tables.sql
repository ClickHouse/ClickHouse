-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

SELECT 'The Default for query_cache_system_table_handling is = throw';
-- Test that the query cache rejects queries that involve system tables.
SELECT * FROM system.one SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT 'Check behavior of query_cache_system_table_handling = throw';
-- Test that the query cache rejects queries that involve system tables.
SELECT * FROM system.one SETTINGS use_query_cache = 1, query_cache_system_table_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT 'Check behavior of query_cache_system_table_handling = save';
-- Test that the query cache saves the result of queries that involve system tables.
SELECT * FROM system.one SETTINGS use_query_cache = 1, query_cache_system_table_handling = 'save';
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT 'Check behavior of query_cache_system_table_handling = ignore';
-- Test that the query cache ignores the result of queries that involve system tables.
SELECT * FROM system.one SETTINGS use_query_cache = 1, query_cache_system_table_handling = 'ignore';
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT 'Other tests';

-- Edge case which doesn't work well due to conceptual reasons (QueryCache is AST-based), test it anyways to have it documented.
USE system;
SELECT * FROM one SETTINGS use_query_cache = 1; -- doesn't throw but should

-- This query uses system.zero internally. Since the query cache works at AST level it does not "see' system.zero and must not complain.
SELECT * SETTINGS use_query_cache = 1;

-- information_schema is also treated as a system table
SELECT * FROM information_schema.tables SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
SELECT * FROM INFORMATION_SCHEMA.TABLES SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }

-- Issue #69010: A system table name appears as a literal. That's okay and must not throw.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (uid Int16, name String) ENGINE = Memory;
SELECT * FROM tab WHERE name = 'system.one' SETTINGS use_query_cache = true;
DROP TABLE tab;

-- System tables can be "hidden" inside e.g. table functions
SELECT * FROM clusterAllReplicas('test_shard_localhost', system.one) SETTINGS use_query_cache = 1; -- {serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
SELECT * FROM clusterAllReplicas('test_shard_localhost', 'system.one') SETTINGS use_query_cache = 1; -- {serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
-- Note how in the previous query ^^ 'system.one' is also a literal. ClusterAllReplicas gets special handling.

-- Criminal edge case that a user creates a table named "system". The query cache must not reject queries against it.
DROP TABLE IF EXISTS system;
CREATE TABLE system (c UInt64) ENGINE = Memory;
SElECT * FROM system SETTINGS use_query_cache = 1;
DROP TABLE system;

-- But queries against system.system are rejected.
DROP TABLE IF EXISTS system.system;
CREATE TABLE system.system (c UInt64) ENGINE = Memory;
SElECT * FROM system.system SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
DROP TABLE system.system;

SYSTEM DROP QUERY CACHE;
