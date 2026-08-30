-- A skip-index substream bundled into skp_idx.packed must report its true uncompressed size in
-- system.data_skipping_indices. The packed archive only stores the compressed size, so the
-- uncompressed size is recovered from the compressed block headers. Before that, packed indices
-- reported uncompressed == compressed, which for a highly compressible bloom filter (identical
-- token in every granule) is off by orders of magnitude.

DROP TABLE IF EXISTS packed_bf;
DROP TABLE IF EXISTS unpacked_bf;

-- Identical data and index; only packing differs. tokenbf_v1 over a single repeated token gives a
-- large uncompressed size (8192 bytes per granule) that compresses to almost nothing.
CREATE TABLE packed_bf
(
    id UInt64,
    s String,
    INDEX bf s TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, packed_skip_index_max_bytes = '4M';

CREATE TABLE unpacked_bf
(
    id UInt64,
    s String,
    INDEX bf s TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, packed_skip_index_max_bytes = 0;

INSERT INTO packed_bf SELECT number, 'a' FROM numbers(100000);
INSERT INTO unpacked_bf SELECT number, 'a' FROM numbers(100000);

-- Uncompressed size is much larger than the compressed size (was equal for packed indices before).
SELECT 'packed_uncompressed_gt_compressed', data_uncompressed_bytes > data_compressed_bytes
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'packed_bf';

-- Packing must not change the reported uncompressed size: it matches the per-file layout exactly.
SELECT 'packed_matches_unpacked',
    (SELECT data_uncompressed_bytes FROM system.data_skipping_indices
     WHERE database = currentDatabase() AND table = 'packed_bf')
  = (SELECT data_uncompressed_bytes FROM system.data_skipping_indices
     WHERE database = currentDatabase() AND table = 'unpacked_bf');

DROP TABLE packed_bf;
DROP TABLE unpacked_bf;
