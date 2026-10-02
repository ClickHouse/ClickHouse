-- Tags: no-fasttest
-- no-fasttest: AES_128_GCM_SIV depends on OpenSSL.

-- The structural substreams (`.size`, `.null`, `.dict`, `.size0`) must be encrypted like the values.

DROP TABLE IF EXISTS t_enc_substreams;

CREATE TABLE t_enc_substreams
(
    s String CODEC(LZ4, AES_128_GCM_SIV),
    n Nullable(UInt64) CODEC(Delta, LZ4, AES_128_GCM_SIV),
    lc LowCardinality(String) CODEC(AES_128_GCM_SIV),
    dd Array(UInt64) CODEC(Delta, Default),
    nd Nullable(String)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, default_compression_codec = 'LZ4, AES_128_GCM_SIV', enable_adaptive_codec_selection = 1,
         serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
         propagate_types_serialization_versions_to_nested_types = 1;

INSERT INTO t_enc_substreams SELECT 'secret', number, 'x', [number, number], 'secret' FROM numbers(1000);
OPTIMIZE TABLE t_enc_substreams FINAL;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_enc_substreams)
WHERE column IN ('s', 'n', 'lc', 'dd', 'nd') -- skip the randomized `_block_number` / `_block_offset` columns
ORDER BY column, substream;

SELECT count(), sum(n), countIf(s = 'secret' AND lc = 'x' AND nd = 'secret' AND arraySum(dd) = 2 * n) FROM t_enc_substreams;
CHECK TABLE t_enc_substreams SETTINGS check_query_single_value_result = 1;

DROP TABLE t_enc_substreams;
