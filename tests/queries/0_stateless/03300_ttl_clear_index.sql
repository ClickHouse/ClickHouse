-- Tags: no-parallel
-- no-parallel: uses global ProfileEvents counters to prove the metadata-only cleanup path.
DROP TABLE IF EXISTS ttl_clear_index;
DROP TABLE IF EXISTS ttl_clear_index_bad;
DROP TABLE IF EXISTS ttl_clear_index_same_expr;
DROP TABLE IF EXISTS ttl_clear_index_packed_all;
DROP TABLE IF EXISTS ttl_clear_index_packed_mixed;
DROP TABLE IF EXISTS ttl_clear_index_large_part;
DROP TABLE IF EXISTS ttl_clear_index_uuid_ineligible;

CREATE TABLE ttl_clear_index_bad
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX missing_idx; -- { serverError BAD_ARGUMENTS }

CREATE TABLE ttl_clear_index
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

INSERT INTO ttl_clear_index VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2);

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index'
  AND active;

SELECT index_clear_ttl_info.index_name[1], length(index_clear_ttl_info.expression), length(index_clear_ttl_info.min), length(index_clear_ttl_info.max)
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index'
  AND active;

CREATE TEMPORARY TABLE ttl_clear_index_events_before (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_events_before
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

OPTIMIZE TABLE ttl_clear_index FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(value) - (SELECT value FROM ttl_clear_index_events_before) > 0
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

CREATE TEMPORARY TABLE ttl_clear_index_events_after (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_events_after
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index'
  AND active;

OPTIMIZE TABLE ttl_clear_index FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(value) = (SELECT value FROM ttl_clear_index_events_after)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

ALTER TABLE ttl_clear_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index'
  AND active;

OPTIMIZE TABLE ttl_clear_index FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index'
  AND active;

SELECT count()
FROM ttl_clear_index
WHERE v = 2;

CREATE TABLE ttl_clear_index_same_expr
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx_a v TYPE minmax GRANULARITY 1,
    INDEX idx_b k TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx_a,
    d + INTERVAL 1 DAY CLEAR INDEX idx_b
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO ttl_clear_index_same_expr VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2);

SELECT arrayStringConcat(index_clear_ttl_info.index_name, ',')
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_same_expr'
  AND active;

OPTIMIZE TABLE ttl_clear_index_same_expr FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_same_expr'
  AND active;

CREATE TABLE ttl_clear_index_packed_all
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
    min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = '1Mi';

INSERT INTO ttl_clear_index_packed_all VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2);

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_packed_all'
  AND active;

CREATE TEMPORARY TABLE ttl_clear_index_packed_events_before (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_packed_events_before
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

OPTIMIZE TABLE ttl_clear_index_packed_all FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(value) - (SELECT value FROM ttl_clear_index_packed_events_before) > 0
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

CREATE TEMPORARY TABLE ttl_clear_index_packed_events_after (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_packed_events_after
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_packed_all'
  AND active;

OPTIMIZE TABLE ttl_clear_index_packed_all FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(value) = (SELECT value FROM ttl_clear_index_packed_events_after)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

CREATE TABLE ttl_clear_index_packed_mixed
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx_expired v TYPE minmax GRANULARITY 1,
    INDEX idx_keep k TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx_expired
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = '1Mi';

INSERT INTO ttl_clear_index_packed_mixed VALUES
    ('2000-01-01', 1, 1),
    ('2000-01-01', 2, 2);

OPTIMIZE TABLE ttl_clear_index_packed_mixed FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_packed_mixed'
  AND active;

SELECT count()
FROM ttl_clear_index_packed_mixed
WHERE k = 2;

DROP TABLE ttl_clear_index;
DROP TABLE ttl_clear_index_same_expr;
DROP TABLE ttl_clear_index_packed_all;
CREATE TABLE ttl_clear_index_large_part
(
    d Date,
    k UInt64,
    v String,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    max_bytes_to_merge_at_min_space_in_pool = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ttl_clear_index_large_part VALUES
    ('2000-01-01', 1, repeat('x', 1024)),
    ('2000-01-01', 2, repeat('y', 1024));

CREATE TEMPORARY TABLE ttl_clear_index_large_events_before (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_large_events_before
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

OPTIMIZE TABLE ttl_clear_index_large_part FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(value) - (SELECT value FROM ttl_clear_index_large_events_before) > 0
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_large_part'
  AND active;

DROP TABLE ttl_clear_index_packed_mixed;
CREATE TABLE ttl_clear_index_uuid_ineligible
(
    d Date,
    k UInt64,
    v String,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    max_bytes_to_merge_at_min_space_in_pool = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ttl_clear_index_uuid_ineligible VALUES
    ('2000-01-01', 1, repeat('x', 1024)),
    ('2000-01-01', 2, repeat('y', 1024));

ALTER TABLE ttl_clear_index_uuid_ineligible MODIFY SETTING assign_part_uuids = 1;

CREATE TEMPORARY TABLE ttl_clear_index_uuid_events_before (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_uuid_events_before
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

OPTIMIZE TABLE ttl_clear_index_uuid_ineligible FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

SELECT sum(value) = (SELECT value FROM ttl_clear_index_uuid_events_before)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_uuid_ineligible'
  AND active;

DROP TABLE ttl_clear_index_large_part;
DROP TABLE ttl_clear_index_uuid_ineligible;
