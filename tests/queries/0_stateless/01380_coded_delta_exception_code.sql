CREATE TABLE delta_codec_synthetic (`id` UInt64 NULL CODEC(Delta, ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }
CREATE TABLE delta_codec_synthetic (`id` UInt64 NULL CODEC(DoubleDelta, ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }
