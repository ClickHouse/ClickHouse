DROP TABLE IF EXISTS ts_codec_uint32;

CREATE TABLE ts_codec_uint32
(
    sample_time UInt32 CODEC(TimeSeriesSamples, ZSTD(3)),
    samples Array(Tuple(UInt32, Float32)) CODEC(TimeSeriesSamples, ZSTD(3))
)
ENGINE = MergeTree
ORDER BY sample_time;

INSERT INTO ts_codec_uint32 VALUES
    (100, [(101, 1.5), (100, 2.25)]),
    (200, [(200, 3.5)]);

SELECT sample_time, sample.1 AS timestamp, sample.2 AS value
FROM ts_codec_uint32
ARRAY JOIN samples AS sample
ORDER BY sample_time, timestamp;

DROP TABLE ts_codec_uint32;
