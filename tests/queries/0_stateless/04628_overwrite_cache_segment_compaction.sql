DROP TABLE IF EXISTS overwrite_cache_segment_compaction;

CREATE TABLE overwrite_cache_segment_compaction
(
    key UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS max_memory_bytes = 100000000;

INSERT INTO overwrite_cache_segment_compaction
SELECT number, 1, repeat('x', 100)
FROM numbers(1000);

CREATE TEMPORARY TABLE overwrite_cache_segment_bytes_before
ENGINE = Memory
AS SELECT total_bytes AS bytes
FROM system.tables
WHERE database = currentDatabase() AND name = 'overwrite_cache_segment_compaction';

INSERT INTO overwrite_cache_segment_compaction
SELECT number, 2, repeat('y', 100)
FROM numbers(600);

SELECT 'compacted', current.total_bytes < previous.bytes * 3 / 2
FROM system.tables AS current
CROSS JOIN overwrite_cache_segment_bytes_before AS previous
WHERE current.database = currentDatabase() AND current.name = 'overwrite_cache_segment_compaction';
SELECT 'new-row', version, payload = repeat('y', 100)
FROM overwrite_cache_segment_compaction
WHERE key = 100;
SELECT 'retained-row', version, payload = repeat('x', 100)
FROM overwrite_cache_segment_compaction
WHERE key = 800;

DROP TABLE overwrite_cache_segment_compaction;
