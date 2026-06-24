DROP TABLE IF EXISTS ttl_clear_index_correctness;

CREATE TABLE ttl_clear_index_correctness
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO ttl_clear_index_correctness VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2);

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_correctness'
  AND active;

OPTIMIZE TABLE ttl_clear_index_correctness FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_correctness'
  AND active;

SELECT count()
FROM ttl_clear_index_correctness
WHERE v = 2;

CHECK TABLE ttl_clear_index_correctness;

DROP TABLE ttl_clear_index_correctness;
