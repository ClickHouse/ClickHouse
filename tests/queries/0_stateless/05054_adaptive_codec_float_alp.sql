-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: block-count assertions depend on the default codec, and sparse randomization adds substream rows.

DROP TABLE IF EXISTS t_float_alp;

CREATE TABLE t_float_alp
(
    d Float64, -- ALP(STD) expected
    f Float32, -- Float32 mirror of d
    r Float64  -- ALP(RD) extected
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'LZ4';

INSERT INTO t_float_alp SELECT
    (cityHash64(number) % 100000) / 100,
    (cityHash64(number) % 10000) / 100,
    2 + cityHash64(number) / 2e19
FROM numbers(300000);

OPTIMIZE TABLE t_float_alp FINAL; -- inserts aren't adaptive, merges are

SELECT column, substream, arraySort(mapKeys(codec_block_counts))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_float_alp)
ORDER BY column, substream;

SELECT count(), max(d), max(f), max(r) FROM t_float_alp;
CHECK TABLE t_float_alp SETTINGS check_query_single_value_result = 1;

DROP TABLE t_float_alp;
