DROP TABLE IF EXISTS delta_table;
DROP TABLE IF EXISTS zstd_table;
DROP TABLE IF EXISTS lz4_table;

CREATE TABLE delta_table (`id` UInt64 CODEC(Delta(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError ILLEGAL_CODEC_PARAMETER}
CREATE TABLE zstd_table (`id` UInt64 CODEC(ZSTD(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError ILLEGAL_CODEC_PARAMETER}
CREATE TABLE lz4_table (`id` UInt64 CODEC(LZ4HC(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError ILLEGAL_CODEC_PARAMETER}

CREATE TABLE lz4_table (`id` UInt64 CODEC(LZ4(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError DATA_TYPE_CANNOT_HAVE_ARGUMENTS}

SELECT 1;

DROP TABLE IF EXISTS delta_table;
DROP TABLE IF EXISTS zstd_table;
DROP TABLE IF EXISTS lz4_table;
