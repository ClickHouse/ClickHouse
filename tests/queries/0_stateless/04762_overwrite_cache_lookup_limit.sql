-- A `LIMIT` lets the read take a lookup index in slices instead of collecting every identifier the
-- predicate matches. The limit reaches the read as a hint, and a step above it may filter rows the
-- storage cannot evaluate, so a read that stopped after that many matches would return too few. Every
-- query below therefore checks that the full number of rows still comes back.

DROP TABLE IF EXISTS overwrite_cache_limit;

CREATE TABLE overwrite_cache_limit
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

INSERT INTO overwrite_cache_limit
SELECT
    toUInt8(number % 4) AS shard,
    toUInt8(number % 8) AS bucket,
    number AS user_id,
    1 AS version,
    concat('p', toString(number)) AS payload
FROM numbers(4000);

SELECT 'plain limit', count() FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 10);
SELECT 'limit one', count() FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 1);
SELECT 'limit above result size', count() FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 5000);
SELECT 'limit with offset', count() FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 10 OFFSET 990);

-- The storage evaluates `shard`, but nothing else. One row in 250 of the index entries survives the
-- residual predicate, so serving this from slices takes several of them.
SELECT 'residual filter', count(), countIf(shard = 1), countIf((user_id % 1000) = 1)
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 AND (user_id % 1000) = 1 LIMIT 3);

-- Asking for more than survives the residual predicate must return everything that does.
SELECT 'residual filter exhausted', count()
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 AND (user_id % 1000) = 1 LIMIT 50);

SELECT 'residual filter with offset', count()
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 AND (user_id % 1000) = 1 LIMIT 2 OFFSET 2);

-- The same through an index intersection, where a slice can lose every identifier it collected.
SELECT 'intersection limit', count(), countIf(shard = 1), countIf(bucket = 5)
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 AND bucket = 5 LIMIT 7);

SELECT 'intersection residual filter', count()
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 AND bucket = 5 AND (user_id % 1000) = 5 LIMIT 3);

-- Tombstones stay in a posting until pruning, so a slice can resolve to no rows at all while later
-- slices still hold some.
DELETE FROM overwrite_cache_limit WHERE shard = 1 AND user_id < 2000;

SELECT 'after delete, limit', count() FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 10);
SELECT 'after delete, all', count() FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 5000);
SELECT 'after delete, residual filter', count(), countIf(user_id >= 2000)
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 AND (user_id % 1000) = 1 LIMIT 5);

-- A limited read must agree with the unlimited one about what it is a prefix of.
SELECT 'limit is a subset', count(), countIf(shard = 1)
FROM (SELECT * FROM overwrite_cache_limit WHERE shard = 1 LIMIT 25);

DROP TABLE overwrite_cache_limit;
