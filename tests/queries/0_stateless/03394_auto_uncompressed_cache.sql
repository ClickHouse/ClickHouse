-- Tags: no-parallel, no-random-settings

DROP TABLE IF EXISTS auto_uncompressed_cache;
DROP TABLE IF EXISTS auto_uncompressed_cache_events;

CREATE TABLE auto_uncompressed_cache
(
    id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE auto_uncompressed_cache_events
(
    event String,
    value UInt64
)
ENGINE = Memory;

INSERT INTO auto_uncompressed_cache
SELECT number, repeat('x', 128)
FROM numbers(100000);

SYSTEM DROP UNCOMPRESSED CACHE;

TRUNCATE TABLE auto_uncompressed_cache_events;
INSERT INTO auto_uncompressed_cache_events
SELECT event, value
FROM system.events
WHERE event IN ('UncompressedCacheHits', 'UncompressedCacheMisses');

SELECT sum(length(value))
FROM auto_uncompressed_cache
WHERE id < 50000
FORMAT Null;

SELECT
    (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM system.events WHERE event = 'UncompressedCacheMisses')
    - (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM auto_uncompressed_cache_events WHERE event = 'UncompressedCacheMisses') > 0;

TRUNCATE TABLE auto_uncompressed_cache_events;
INSERT INTO auto_uncompressed_cache_events
SELECT event, value
FROM system.events
WHERE event = 'UncompressedCacheHits';

SELECT sum(length(value))
FROM auto_uncompressed_cache
WHERE id < 50000
FORMAT Null;

SELECT
    (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM system.events WHERE event = 'UncompressedCacheHits')
    - (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM auto_uncompressed_cache_events WHERE event = 'UncompressedCacheHits') > 0;

SYSTEM DROP UNCOMPRESSED CACHE;
SET use_uncompressed_cache = 0;

TRUNCATE TABLE auto_uncompressed_cache_events;
INSERT INTO auto_uncompressed_cache_events
SELECT event, value
FROM system.events
WHERE event IN ('UncompressedCacheHits', 'UncompressedCacheMisses');

SELECT sum(length(value))
FROM auto_uncompressed_cache
WHERE id < 50000
FORMAT Null;

SELECT sum(length(value))
FROM auto_uncompressed_cache
WHERE id < 50000
FORMAT Null;

SELECT
    (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM system.events WHERE event = 'UncompressedCacheHits')
    - (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM auto_uncompressed_cache_events WHERE event = 'UncompressedCacheHits'),
    (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM system.events WHERE event = 'UncompressedCacheMisses')
    - (SELECT ifNull(anyOrNull(value), toUInt64(0)) FROM auto_uncompressed_cache_events WHERE event = 'UncompressedCacheMisses');

DROP TABLE auto_uncompressed_cache;
DROP TABLE auto_uncompressed_cache_events;
