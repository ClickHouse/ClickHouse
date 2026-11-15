-- A single call that looks up thousands of keys at once - the shape `overwriteCacheGet` takes over a
-- block of many rows - must resolve every key independently of how many others share its batch: a live
-- key, a key whose primary-index entry now points to a tombstone left by `DELETE`, and a key that was
-- never inserted at all must not be confused with one another once the lookup groups keys by shard.

DROP TABLE IF EXISTS overwrite_cache_batched_lookup;

CREATE TABLE overwrite_cache_batched_lookup
(
    key UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (key);

-- Enough distinct keys to spread across every shard of the primary index.
INSERT INTO overwrite_cache_batched_lookup SELECT number, 1, concat('v', toString(number)) FROM numbers(6000);

-- Every third key is deleted, so its primary-index entry survives with a tombstone as its head version.
DELETE FROM overwrite_cache_batched_lookup WHERE key IN (SELECT number * 3 FROM numbers(2000));

SELECT '-- live, tombstoned and never-inserted keys resolve independently within one batched lookup';
SELECT
    countIf(number < 6000 AND number % 3 != 0
        AND overwriteCacheGetOrNull('overwrite_cache_batched_lookup', 'payload', number) IS NOT NULL) AS live,
    countIf(number < 6000 AND number % 3 = 0
        AND overwriteCacheGetOrNull('overwrite_cache_batched_lookup', 'payload', number) IS NULL) AS tombstoned,
    countIf(number >= 6000
        AND overwriteCacheGetOrNull('overwrite_cache_batched_lookup', 'payload', number) IS NULL) AS absent
FROM numbers(8000);

SELECT '-- resolved payloads still match what was inserted';
SELECT countIf(overwriteCacheGetOrNull('overwrite_cache_batched_lookup', 'payload', number) != concat('v', toString(number)))
FROM numbers(8000)
WHERE number < 6000 AND number % 3 != 0;

SELECT '-- a batched `WHERE key IN (...)` read over the same mix behaves the same way';
SELECT count() FROM overwrite_cache_batched_lookup WHERE key IN (SELECT number FROM numbers(8000));

-- The count alone would not notice a read that returned the right number of rows with their columns
-- attributed to the wrong keys, which is what probing the primary index out of key order risks.
SELECT '-- every returned row still carries the payload of its own key';
SELECT countIf(payload != concat('v', toString(key))), min(key), max(key)
FROM overwrite_cache_batched_lookup
WHERE key IN (SELECT number FROM numbers(8000));

DROP TABLE overwrite_cache_batched_lookup;
