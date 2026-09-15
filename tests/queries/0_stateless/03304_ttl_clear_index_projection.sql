DROP TABLE IF EXISTS ttl_clear_index_projection;

CREATE TABLE ttl_clear_index_projection
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1,
    PROJECTION by_v
    (
        SELECT k, v
        ORDER BY v
    )
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO ttl_clear_index_projection VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2),
    ('2000-01-01', 3, 3),
    ('2000-01-01', 4, 4);

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_projection'
  AND active;

SELECT length(projections)
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_projection'
  AND active;

OPTIMIZE TABLE ttl_clear_index_projection FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_projection'
  AND active;

SELECT length(projections)
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_projection'
  AND active;

SELECT count()
FROM ttl_clear_index_projection
WHERE v >= 2;

DROP TABLE ttl_clear_index_projection;
