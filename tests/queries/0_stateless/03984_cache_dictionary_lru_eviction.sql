-- Tags: no-parallel

-- Test that cache dictionary evicts least recently accessed cells (LRU behavior).
-- Strategy:
-- 1. Fill a small cache with keys, caching their values.
-- 2. Update the source table so cached values become stale.
--    The cache still holds the OLD values because the lifetime is long (nothing expires).
-- 3. Touch some keys (cache hits) to refresh their access time.
-- 4. Force eviction by accessing many new keys.
-- 5. Touched keys should still return the OLD cached value (survived eviction).
--    Untouched keys should return the NEW value (were evicted and re-fetched from the updated source).

DROP DATABASE IF EXISTS test_03984;
CREATE DATABASE test_03984;

-- Source table
CREATE TABLE test_03984.source (id UInt64, value String) ENGINE = Memory;

-- Insert initial data
INSERT INTO test_03984.source SELECT number, 'old_' || toString(number) FROM numbers(200);

-- Small cache dictionary: 128 cells, long lifetime so nothing expires during the test
CREATE DICTIONARY test_03984.cache_dict
(
    id UInt64,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source' DB 'test_03984'))
LIFETIME(MIN 600 MAX 1200)
LAYOUT(CACHE(SIZE_IN_CELLS 128));

-- Step 1: Load keys 0-127 into the cache
SELECT dictGet('test_03984.cache_dict', 'value', number) FROM numbers(128) FORMAT Null;

-- Step 2: Update source data so we can detect which keys are still cached vs re-fetched
TRUNCATE TABLE test_03984.source;
INSERT INTO test_03984.source SELECT number, 'new_' || toString(number) FROM numbers(200);

-- Step 3: Touch keys 0-9 (cache hits, refreshes their last_access_time)
SELECT dictGet('test_03984.cache_dict', 'value', number) FROM numbers(10) FORMAT Null;

-- Step 4: Access new keys 128-199 to force eviction of old cells
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(number + 128)) FROM numbers(72) FORMAT Null;

-- Step 5: Check that the touched keys (0-9) survived eviction (should still return old cached values).
-- All 10 touched keys must survive: they have a newer last_access_time than the 118 untouched keys,
-- and we only evict 72 cells.
SELECT 'touched_keys_cached',
    sum(dictGet('test_03984.cache_dict', 'value', number) LIKE 'old_%') = 10
FROM numbers(10);

-- Step 6: For comparison, check the untouched keys (64-127) — many should have been evicted
-- and re-fetched with the new value.
SELECT 'untouched_keys_evicted',
    sum(dictGet('test_03984.cache_dict', 'value', toUInt64(number + 64)) LIKE 'new_%') >= 1
FROM numbers(64);

DROP DICTIONARY test_03984.cache_dict;
DROP TABLE test_03984.source;
DROP DATABASE test_03984;
