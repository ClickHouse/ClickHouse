-- Tests ALTER TABLE ... RECOMPRESS COLUMN for wide-part columns whose *explicit* codec AST references
-- `Default` (directly, CODEC(Default), or inside a pipeline, CODEC(Delta, Default)). `Default` resolves
-- through the table's `default_compression_codec` setting, so after changing that setting RECOMPRESS
-- COLUMN must re-compress the stored data with the new effective codec. The wide in-place fast path
-- resolves the codec against the part's *stored* default codec and cannot honor the change, so such
-- columns must fall back to a whole-part rewrite (the same as a column with no explicit CODEC).

SET mutations_sync = 2;
SET check_query_single_value_result = 1;

-- CODEC(Default) on a Wide part.
DROP TABLE IF EXISTS t_recompress_codec_default;

CREATE TABLE t_recompress_codec_default (id UInt64, s String CODEC(Default))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'NONE';

INSERT INTO t_recompress_codec_default SELECT number, repeat('a', 100) FROM numbers(100000);

SELECT DISTINCT 'default part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_codec_default' AND active;
SELECT 'default none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_codec_default' AND column = 's' AND active;

-- Change the table default codec (metadata-only). CODEC(Default) now resolves to ZSTD.
ALTER TABLE t_recompress_codec_default MODIFY SETTING default_compression_codec = 'ZSTD';
ALTER TABLE t_recompress_codec_default RECOMPRESS COLUMN s;

SELECT 'default after', count(), countIf(s = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_codec_default;
SELECT 'default zstd is small', sum(data_compressed_bytes) < 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_codec_default' AND column = 's' AND active;

CHECK TABLE t_recompress_codec_default;

DROP TABLE t_recompress_codec_default;

-- CODEC(Delta, Default) pipeline on a Wide part: the generic tail (Default) also follows the setting.
DROP TABLE IF EXISTS t_recompress_codec_delta_default;

CREATE TABLE t_recompress_codec_delta_default (id UInt64, v UInt64 CODEC(Delta, Default))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'NONE';

INSERT INTO t_recompress_codec_delta_default SELECT number, number FROM numbers(100000);

SELECT DISTINCT 'delta part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_codec_delta_default' AND active;
SELECT 'delta none is large', sum(data_compressed_bytes) > 700000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_codec_delta_default' AND column = 'v' AND active;

ALTER TABLE t_recompress_codec_delta_default MODIFY SETTING default_compression_codec = 'ZSTD';
ALTER TABLE t_recompress_codec_delta_default RECOMPRESS COLUMN v;

SELECT 'delta after', count(), countIf(v = id), sum(v) FROM t_recompress_codec_delta_default;
SELECT 'delta zstd is small', sum(data_compressed_bytes) < 200000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_codec_delta_default' AND column = 'v' AND active;

CHECK TABLE t_recompress_codec_delta_default;

DROP TABLE t_recompress_codec_delta_default;
