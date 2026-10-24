-- `RECOMPRESS COLUMN` must detect a column codec referencing `Default` case-insensitively, the same
-- way `CompressionCodecFactory::get` resolves it. For a heterogeneous `Tuple` the original codec AST
-- is kept as written (`Delta` needs different arguments for the elements), so a lowercase `default`
-- reaches the guard unchanged. Such a column must go through the whole-part rewrite to honor a changed
-- `default_compression_codec`.

SET mutations_sync = 2;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_recompress_lowercase_default;

CREATE TABLE t_recompress_lowercase_default (v Tuple(UInt32, UInt64) CODEC(Delta, default))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
         min_level_for_full_part_storage = 0, default_compression_codec = 'NONE';

INSERT INTO t_recompress_lowercase_default SELECT (toUInt32(number), number) FROM numbers(100000);

SELECT DISTINCT 'part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_lowercase_default' AND active;
SELECT 'none is large', sum(data_compressed_bytes) > 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_lowercase_default' AND column = 'v' AND active;

ALTER TABLE t_recompress_lowercase_default MODIFY SETTING default_compression_codec = 'ZSTD';
ALTER TABLE t_recompress_lowercase_default RECOMPRESS COLUMN v;

SELECT 'after', count(), sum(v.1), sum(v.2) FROM t_recompress_lowercase_default;
SELECT 'zstd is small', sum(data_compressed_bytes) < 200000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_lowercase_default' AND column = 'v' AND active;

CHECK TABLE t_recompress_lowercase_default;

DROP TABLE t_recompress_lowercase_default;
