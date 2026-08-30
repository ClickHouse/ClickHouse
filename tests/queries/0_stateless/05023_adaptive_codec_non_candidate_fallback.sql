-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: to avoid randomized sparse serialization

DROP TABLE IF EXISTS t_non_candidate_fallback;

-- String has no specialized adaptive candidate, so its pool is NONE + the default codec.
CREATE TABLE t_non_candidate_fallback
(
    incompressible String,  -- pseudo-random bytes the default codec expands -> NONE
    compressible String     -- repetitive bytes the default codec shrinks -> LZ4
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1;

INSERT INTO t_non_candidate_fallback SELECT reinterpretAsString(sipHash128(number)), repeat('a', 16) FROM numbers(100000);
INSERT INTO t_non_candidate_fallback SELECT reinterpretAsString(sipHash128(number)), repeat('a', 16) FROM numbers(100000, 100000);
OPTIMIZE TABLE t_non_candidate_fallback FINAL;

SELECT column, substream, codec_block_counts FROM mergeTreeCodecBlockCounts(currentDatabase(), t_non_candidate_fallback) ORDER BY column, substream;

SELECT count(), sum(length(incompressible)), sum(length(compressible)) FROM t_non_candidate_fallback;

DROP TABLE t_non_candidate_fallback;
