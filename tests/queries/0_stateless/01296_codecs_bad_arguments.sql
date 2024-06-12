DROP TABLE IF EXISTS delta_table;
DROP TABLE IF EXISTS zstd_table;
DROP TABLE IF EXISTS lz4_table;

CREATE TABLE delta_table (`id` UInt64 CODEC(Delta(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 433}
CREATE TABLE zstd_table (`id` UInt64 CODEC(ZSTD(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 433}
CREATE TABLE lz4_table (`id` UInt64 CODEC(LZ4HC(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 433}

CREATE TABLE lz4_table (`id` UInt64 CODEC(LZ4(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 378}

SELECT 1;

DROP TABLE IF EXISTS delta_table;
DROP TABLE IF EXISTS zstd_table;
DROP TABLE IF EXISTS lz4_table;
