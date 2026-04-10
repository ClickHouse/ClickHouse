-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests query batching settings validation and behavior.

SYSTEM CLEAR QUERY CACHE;

SELECT 'Test 1: query_batching_max_wait_ms setting is accepted';

DROP TABLE IF EXISTS test_batching_settings;
CREATE TABLE test_batching_settings (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_batching_settings SELECT number FROM numbers(100);

-- Setting a custom max wait time should work without errors.
SELECT count() FROM test_batching_settings
    SETTINGS use_query_batching = 1, query_batching_max_wait_ms = 50;

SELECT 'Test 2: query_batching_max_batch_size setting is accepted';

-- Setting a custom batch size should work without errors.
SELECT count() FROM test_batching_settings
    SETTINGS use_query_batching = 1, query_batching_max_batch_size = 10;

SELECT 'Test 3: Batching with query cache coexistence';

SYSTEM CLEAR QUERY CACHE;

-- Both settings can be used together.
SELECT count() FROM test_batching_settings
    SETTINGS use_query_batching = 1, use_query_cache = 1, query_cache_ttl = 5;

-- The cache should have an entry.
SELECT count() >= 1 FROM system.query_cache;

DROP TABLE test_batching_settings;

SYSTEM CLEAR QUERY CACHE;
