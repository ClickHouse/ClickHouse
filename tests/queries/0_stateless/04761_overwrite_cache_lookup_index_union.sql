-- Reading a lookup index for several values merges one sorted posting per value, and reading several
-- indexes intersects the merged results. Both are checked against value counts that make an unmerged,
-- duplicated or truncated run visible.

DROP TABLE IF EXISTS overwrite_cache_union;

CREATE TABLE overwrite_cache_union
(
    shard UInt8,
    bucket UInt8,
    user_id UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (shard, bucket, user_id)
INDEX (shard), (bucket)
SETTINGS max_memory_bytes = 268435456, persist_mode = 'none';

INSERT INTO overwrite_cache_union
SELECT
    toUInt8(number % 5) AS shard,
    toUInt8(number % 7) AS bucket,
    number AS user_id,
    1 AS version,
    'p' AS payload
FROM numbers(700);

SELECT 'one value', count(), uniqExact(user_id) FROM overwrite_cache_union WHERE shard = 0;
SELECT 'two values', count(), uniqExact(user_id) FROM overwrite_cache_union WHERE shard IN (0, 1);
SELECT 'three values', count(), uniqExact(user_id) FROM overwrite_cache_union WHERE shard IN (0, 1, 2);
SELECT 'four values', count(), uniqExact(user_id) FROM overwrite_cache_union WHERE shard IN (0, 1, 2, 3);
SELECT 'five values', count(), uniqExact(user_id) FROM overwrite_cache_union WHERE shard IN (0, 1, 2, 3, 4);

-- A repeated value must not contribute its posting twice.
SELECT 'repeated value', count() FROM overwrite_cache_union WHERE shard IN (2, 2, 2);
SELECT 'unknown value', count() FROM overwrite_cache_union WHERE shard IN (0, 200);

-- Intersecting a union of one index with a union of another.
SELECT 'intersection', count() FROM overwrite_cache_union WHERE shard = 3 AND bucket = 4;
SELECT 'union intersection', count() FROM overwrite_cache_union WHERE shard IN (1, 3) AND bucket IN (2, 4, 6);
SELECT 'empty intersection', count() FROM overwrite_cache_union WHERE shard IN (1, 3) AND bucket IN (200);

-- The same shape once the postings hold tombstones, so that a deleted identifier is dropped by every
-- posting that carried it rather than by one of them.
DELETE FROM overwrite_cache_union WHERE shard = 1;

SELECT 'after delete, one value', count() FROM overwrite_cache_union WHERE shard = 1;
SELECT 'after delete, union', count() FROM overwrite_cache_union WHERE shard IN (0, 1, 2);
SELECT 'after delete, intersection', count() FROM overwrite_cache_union WHERE shard IN (0, 1, 2) AND bucket IN (3, 5);

-- Reinserting restores the rows in the same postings.
INSERT INTO overwrite_cache_union
SELECT
    toUInt8(number % 5) AS shard,
    toUInt8(number % 7) AS bucket,
    number AS user_id,
    2 AS version,
    'q' AS payload
FROM numbers(700)
WHERE (number % 5) = 1;

SELECT 'after reinsert, one value', count(), uniqExact(user_id), uniqExact(payload) FROM overwrite_cache_union WHERE shard = 1;
SELECT 'after reinsert, union', count(), uniqExact(user_id) FROM overwrite_cache_union WHERE shard IN (0, 1, 2);

DROP TABLE overwrite_cache_union;
