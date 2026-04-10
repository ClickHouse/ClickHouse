-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests basic query batching functionality.
-- When use_query_batching is enabled, identical concurrent queries should be
-- batched together, with the leader writing to the query result cache and
-- followers reading from it.

SYSTEM CLEAR QUERY CACHE;

SELECT 'Test 1: Basic query batching enabled - query executes correctly';

DROP TABLE IF EXISTS test_batching;
CREATE TABLE test_batching (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_batching VALUES (1, 'hello'), (2, 'world'), (3, 'foo');

-- With batching enabled, a simple SELECT should still return correct results.
SELECT id, value FROM test_batching ORDER BY id SETTINGS use_query_batching = 1;

SELECT 'Test 2: Query batching writes to query result cache';

SYSTEM CLEAR QUERY CACHE;

-- Run the same query twice with batching. The first execution should populate the cache.
SELECT count() FROM test_batching SETTINGS use_query_batching = 1;
SELECT count() FROM test_batching SETTINGS use_query_batching = 1;

-- The query result cache should have an entry now.
SELECT count() >= 1 FROM system.query_cache;

SELECT 'Test 3: Query batching is disabled by default';

SYSTEM CLEAR QUERY CACHE;

-- Without use_query_batching, the cache should not be populated by the batching mechanism.
SELECT count() FROM test_batching;
SELECT count() FROM system.query_cache;

SELECT 'Test 4: Different queries are not batched together';

SYSTEM CLEAR QUERY CACHE;

SELECT count() FROM test_batching WHERE id = 1 SETTINGS use_query_batching = 1;
SELECT count() FROM test_batching WHERE id = 2 SETTINGS use_query_batching = 1;

-- Two different queries should create two separate cache entries.
SELECT count() >= 2 FROM system.query_cache;

SELECT 'Test 5: Non-SELECT queries are not affected';

-- INSERT should work normally with batching enabled.
INSERT INTO test_batching VALUES (4, 'bar') SETTINGS use_query_batching = 1;
SELECT count() FROM test_batching SETTINGS use_query_batching = 1;

DROP TABLE test_batching;

SYSTEM CLEAR QUERY CACHE;
