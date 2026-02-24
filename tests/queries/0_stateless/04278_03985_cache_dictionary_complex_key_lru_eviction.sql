-- Same deterministic clock (approximate LRU) eviction test as
-- 03984_cache_dictionary_lru_eviction.sql but for COMPLEX_KEY_CACHE.
--
-- A 4-cell cache with one key fetched per query fully determines the clock state.
-- The trace (max_clock_count = 3) is:
--   1. fetch 0,1,2,3      -> slots 0..3, clock_hand = 0
--   2. hit 0,1,2,3        -> all clock_count = 3 (recency separation)
--   3. fetch 4 (full)     -> sweep evicts key 0 (LRU), inserts key 4, clock_hand = 1
--   4. fetch 5 (full)     -> with the fix key 4 was inserted at clock_count = 3 (same
--                            recency as a hit) so it survives and key 1 is evicted;
--                            with the buggy low-admission insert (clock_count = 1) key 4
--                            is the lowest-count cell and is evicted here instead.

DROP DATABASE IF EXISTS test_04278;
CREATE DATABASE test_04278;

CREATE TABLE test_04278.source (id UInt64, tag String, value String) ENGINE = Memory;
INSERT INTO test_04278.source
    SELECT number, 'tag_' || toString(number), 'old_' || toString(number)
    FROM numbers(10);

-- 4-cell cache, long lifetime so nothing expires during the test
CREATE DICTIONARY test_04278.cache_dict
(
    id UInt64,
    tag String,
    value String DEFAULT ''
)
PRIMARY KEY id, tag
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source' DB 'test_04278'))
LIFETIME(MIN 600 MAX 1200)
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 4));

-- Step 1: fill the 4 cells with keys 0..3 (one key per query fixes the slot order)
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(0), 'tag_0')) FORMAT Null;
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(1), 'tag_1')) FORMAT Null;
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(2), 'tag_2')) FORMAT Null;
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(3), 'tag_3')) FORMAT Null;

-- Step 2: hit keys 0..3 so every cell reaches max_clock_count (recency separation)
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(0), 'tag_0')) FORMAT Null;
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(1), 'tag_1')) FORMAT Null;
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(2), 'tag_2')) FORMAT Null;
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(3), 'tag_3')) FORMAT Null;

-- Step 3: fetch key 4 while the source is still old -> evicts key 0 (LRU), caches old_4
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(4), 'tag_4')) FORMAT Null;

-- Make source values new so we can tell cached (old) from re-fetched (new)
TRUNCATE TABLE test_04278.source;
INSERT INTO test_04278.source
    SELECT number, 'tag_' || toString(number), 'new_' || toString(number)
    FROM numbers(10);

-- Step 4: fetch key 5 -> triggers another eviction (evicts key 1, key 4 survives)
SELECT dictGet('test_04278.cache_dict', 'value', (toUInt64(5), 'tag_5')) FORMAT Null;

-- Key 4 was fetched right before key 5, so it must still be cached (old value).
SELECT 'recent_insert_survived', dictGet('test_04278.cache_dict', 'value', (toUInt64(4), 'tag_4')) = 'old_4';
-- Key 0 was the least recently used and must have been evicted (re-fetched -> new value).
SELECT 'lru_key_evicted', dictGet('test_04278.cache_dict', 'value', (toUInt64(0), 'tag_0')) = 'new_0';

DROP DICTIONARY test_04278.cache_dict;
DROP TABLE test_04278.source;
DROP DATABASE test_04278;
