-- Tags: no-random-settings

-- When a table sets both the `default_compression_codec` `MergeTree` setting and a
-- `RECOMPRESS` TTL, the `RECOMPRESS` codec must win for the recompressed part: the
-- part metadata (`default_compression_codec.txt`, reported by `system.parts`) and the
-- actual on-disk column data must agree.
-- Previously the writer re-read `default_compression_codec` and overrode the codec
-- selected for the part, so a `RECOMPRESS CODEC(NONE)` merge recorded `NONE` in the
-- metadata while still writing the columns with the setting's codec.
-- https://github.com/ClickHouse/ClickHouse/issues/84440

DROP TABLE IF EXISTS t_codec_recompress_ttl;

CREATE TABLE t_codec_recompress_ttl
(
    dt DateTime,
    s String
)
ENGINE = MergeTree
ORDER BY tuple()
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
SETTINGS default_compression_codec = 'ZSTD(3)', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_codec_recompress_ttl;

-- Highly compressible data inserted with an already-expired TTL.
INSERT INTO t_codec_recompress_ttl SELECT now() - INTERVAL 1 DAY, repeat('a', 100) FROM numbers(100000);

-- Before recompression the configured `default_compression_codec` applies, so the
-- data is heavily compressed.
SELECT 'before merge';
SELECT default_compression_codec, data_compressed_bytes < data_uncompressed_bytes / 10
FROM system.parts
WHERE database = currentDatabase() AND table = 't_codec_recompress_ttl' AND active;

SYSTEM START TTL MERGES t_codec_recompress_ttl;
OPTIMIZE TABLE t_codec_recompress_ttl FINAL;

-- After the `RECOMPRESS CODEC(NONE)` merge the metadata reports `NONE` and the data
-- must actually be uncompressed (compressed size is at least the uncompressed size).
SELECT 'after recompress';
SELECT default_compression_codec, data_compressed_bytes >= data_uncompressed_bytes
FROM system.parts
WHERE database = currentDatabase() AND table = 't_codec_recompress_ttl' AND active;

DROP TABLE t_codec_recompress_ttl;
