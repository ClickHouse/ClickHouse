-- Deterministic test for the cache dictionary clock (approximate LRU) eviction.
--
-- The previous version of this test used a 128-cell cache and multi-key dictGet
-- calls, which made both the insertion order (and therefore the clock-hand
-- position) and the exact eviction outcome non-deterministic, so it was flaky.
--
-- Here we use a 4-cell cache and fetch one key per query, which fully determines
-- the clock state. The trace (max_clock_count = 3) is:
--   1. fetch 0,1,2,3      -> slots 0..3, clock_hand = 0
--   2. hit 0,1,2,3        -> all clock_count = 3 (this separation is what makes a
--                            below-hit insert recency observable)
--   3. fetch 4 (full)     -> sweep decrements all to 2, evicts slot 0 (key 0, the
--                            least recently used), inserts key 4, clock_hand = 1
--   4. fetch 5 (full)     -> sweep from hand 1. With the fix, key 4 was inserted at
--                            clock_count = 3 (same recency as a hit), so it survives
--                            and key 1 is evicted. With the buggy low-admission insert
--                            (clock_count = 1), key 4 is the lowest-count cell and is
--                            evicted here instead.
-- A freshly fetched key must therefore survive the next eviction.

DROP DATABASE IF EXISTS test_03984;
CREATE DATABASE test_03984;

CREATE TABLE test_03984.source (id UInt64, value String) ENGINE = Memory;
INSERT INTO test_03984.source SELECT number, 'old_' || toString(number) FROM numbers(10);

-- 4-cell cache, long lifetime so nothing expires during the test
CREATE DICTIONARY test_03984.cache_dict
(
    id UInt64,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source' DB 'test_03984'))
LIFETIME(MIN 600 MAX 1200)
LAYOUT(CACHE(SIZE_IN_CELLS 4));

-- Step 1: fill the 4 cells with keys 0..3 (one key per query fixes the slot order)
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(0)) FORMAT Null;
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(1)) FORMAT Null;
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(2)) FORMAT Null;
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(3)) FORMAT Null;

-- Step 2: hit keys 0..3 so every cell reaches max_clock_count (recency separation)
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(0)) FORMAT Null;
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(1)) FORMAT Null;
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(2)) FORMAT Null;
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(3)) FORMAT Null;

-- Step 3: fetch key 4 while the source is still old -> evicts key 0 (LRU), caches old_4
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(4)) FORMAT Null;

-- Make source values new so we can tell cached (old) from re-fetched (new)
TRUNCATE TABLE test_03984.source;
INSERT INTO test_03984.source SELECT number, 'new_' || toString(number) FROM numbers(10);

-- Step 4: fetch key 5 -> triggers another eviction (evicts key 1, key 4 survives)
SELECT dictGet('test_03984.cache_dict', 'value', toUInt64(5)) FORMAT Null;

-- Key 4 was fetched right before key 5, so it must still be cached (old value).
SELECT 'recent_insert_survived', dictGet('test_03984.cache_dict', 'value', toUInt64(4)) = 'old_4';
-- Key 0 was the least recently used and must have been evicted (re-fetched -> new value).
SELECT 'lru_key_evicted', dictGet('test_03984.cache_dict', 'value', toUInt64(0)) = 'new_0';

DROP DICTIONARY test_03984.cache_dict;
DROP TABLE test_03984.source;
DROP DATABASE test_03984;
