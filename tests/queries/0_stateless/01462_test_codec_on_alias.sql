DROP TABLE IF EXISTS compression_codec_on_alias;

select 'create table compression_codec_on_alias with CODEC on ALIAS type';

CREATE TABLE compression_codec_on_alias (
    `c0` ALIAS c1 CODEC(ZSTD),
    c1 UInt64
) ENGINE = MergeTree() PARTITION BY c0 ORDER BY c1; -- { serverError 36 }

select 'create table compression_codec_on_alias with proper CODEC';

CREATE TABLE compression_codec_on_alias (
    c0 UInt64 CODEC(ZSTD),
    c1 UInt64
) ENGINE = MergeTree() PARTITION BY c0 ORDER BY c1; -- success

select 'alter table compression_codec_on_alias add column (ALIAS type) with CODEC';

ALTER TABLE compression_codec_on_alias ADD COLUMN `c3` ALIAS c2 CODEC(ZSTD) AFTER c2; -- { serverError 36 }

select 'alter table compression_codec_on_alias add column (NOT ALIAS type) with CODEC';

ALTER TABLE compression_codec_on_alias ADD COLUMN c2 UInt64 CODEC(ZSTD) AFTER c1; -- success

DROP TABLE IF EXISTS compression_codec_on_alias;
