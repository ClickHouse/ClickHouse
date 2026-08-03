-- Tags: no-parallel, no-random-merge-tree-settings
-- no-parallel: compares a global ProfileEvents counter before and after the merge.
-- no-random-merge-tree-settings: the source part must otherwise be eligible for file-preserving index cleanup.

DROP TABLE IF EXISTS ttl_clear_index_recompression_metadata;

CREATE TABLE ttl_clear_index_recompression_metadata
(
    d Date,
    k UInt64,
    v String,
    INDEX idx k TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    default_compression_codec = 'LZ4',
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES ttl_clear_index_recompression_metadata;

INSERT INTO ttl_clear_index_recompression_metadata VALUES
    ('2000-01-01', 1, repeat('a', 1000)),
    ('2000-01-01', 2, repeat('b', 1000));

ALTER TABLE ttl_clear_index_recompression_metadata MODIFY TTL
    d + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1)),
    d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS materialize_ttl_after_modify = 0;

-- The old part has no metadata for the newly added recompression rule.
SELECT empty(recompression_ttl_info.expression)
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_recompression_metadata'
  AND active;

CREATE TEMPORARY TABLE ttl_clear_index_recompression_events_before (value UInt64) ENGINE = Memory;
INSERT INTO ttl_clear_index_recompression_events_before
SELECT sum(value)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

SYSTEM START TTL MERGES ttl_clear_index_recompression_metadata;
OPTIMIZE TABLE ttl_clear_index_recompression_metadata FINAL
SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1;

-- Missing recompression metadata must force a regular rewrite that recalculates all TTL
-- metadata and clears the expired index instead of preserving files.
SELECT notEmpty(recompression_ttl_info.expression), secondary_indices_compressed_bytes
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_recompression_metadata'
  AND active;

SELECT sum(value) = (SELECT value FROM ttl_clear_index_recompression_events_before)
FROM system.events
WHERE event = 'TTLClearIndexMetadataOnlyMerges';

-- Once the first rewrite has populated the metadata, the expired recompression rule is applied.
OPTIMIZE TABLE ttl_clear_index_recompression_metadata FINAL;

SELECT default_compression_codec
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_recompression_metadata'
  AND active;

SELECT count(), sum(k), sum(length(v))
FROM ttl_clear_index_recompression_metadata;

CHECK TABLE ttl_clear_index_recompression_metadata SETTINGS check_query_single_value_result = 1;

DROP TABLE ttl_clear_index_recompression_metadata;
