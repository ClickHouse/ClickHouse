-- Tags: no-fasttest, no-random-merge-tree-settings
-- no-fasttest: AES_128_GCM_SIV depends on OpenSSL.
-- no-random-merge-tree-settings: randomized enable_block_number/offset_column add extra column rows to the output.

-- Adaptive selection must never substitute an encrypting default: that would silently write data the operator told us to encrypt.

DROP TABLE IF EXISTS t_enc;
DROP TABLE IF EXISTS t_plain;

-- x would pick NONE without encryption.
-- y would pick T64 without encryption
CREATE TABLE t_enc
(
    x UInt64,
    y UInt64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1, default_compression_codec = 'LZ4, AES_128_GCM_SIV';

INSERT INTO t_enc SELECT cityHash64(number), number FROM numbers(1000000);
INSERT INTO t_enc SELECT cityHash64(number), number FROM numbers(1000000, 1000000);
OPTIMIZE TABLE t_enc FINAL;

-- Every block must be the encrypting codec.
SELECT 'encrypted', column,
       mapContains(codec_block_counts, 'T64') AS has_t64,
       mapContains(codec_block_counts, 'NONE') AS has_none,
       arrayExists(k -> position(k, 'AES') > 0, mapKeys(codec_block_counts)) AS has_encryption
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_enc) ORDER BY column;

-- The data round-trips through encryption.
SELECT 'read', count(), countIf(x = cityHash64(y)) FROM t_enc;
CHECK TABLE t_enc SETTINGS check_query_single_value_result = 1;

-- Same data, plain default. Adaptive does substitute here.
CREATE TABLE t_plain
(
    x UInt64,
    y UInt64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1, default_compression_codec = 'LZ4';

INSERT INTO t_plain SELECT cityHash64(number), number FROM numbers(1000000);
INSERT INTO t_plain SELECT cityHash64(number), number FROM numbers(1000000, 1000000);
OPTIMIZE TABLE t_plain FINAL;

SELECT 'plain', column,
       mapContains(codec_block_counts, 'T64') AS has_t64,
       mapContains(codec_block_counts, 'NONE') AS has_none
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_plain) ORDER BY column;

DROP TABLE t_enc;
DROP TABLE t_plain;
