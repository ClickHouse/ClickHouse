DROP TABLE IF EXISTS ttl_clear_index_dependencies;

CREATE TABLE ttl_clear_index_dependencies
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO ttl_clear_index_dependencies VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2);

ALTER TABLE ttl_clear_index_dependencies
    MODIFY TTL d + INTERVAL 1 DAY CLEAR INDEX idx
    SETTINGS mutations_sync = 2;

SELECT
    length(index_clear_ttl_info.expression),
    toYear(index_clear_ttl_info.max[1])
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_dependencies'
  AND active;

ALTER TABLE ttl_clear_index_dependencies
    UPDATE d = toDate('2100-01-01') WHERE 1
    SETTINGS mutations_sync = 2;

SELECT
    length(index_clear_ttl_info.expression),
    toYear(index_clear_ttl_info.max[1])
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_dependencies'
  AND active;

DROP TABLE ttl_clear_index_dependencies;
