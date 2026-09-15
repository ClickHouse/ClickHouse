-- A codec family name is matched case-insensitively, the way the `CODEC` keyword itself is. Without that, a codec whose
-- canonical name is mixed-case (`Delta`, `DoubleDelta`, `Gorilla`, ...) could not be named at all in the
-- codec-valued `MergeTree` settings or in the `<compression>` server config, because those paths used to
-- upper-case the whole codec expression before parsing it.

DROP TABLE IF EXISTS t_codec_case;
DROP TABLE IF EXISTS t_codec_case_settings;

SELECT '-- a column CODEC accepts any spelling and stores the canonical description';

CREATE TABLE t_codec_case
(
    a UInt64 CODEC(zstd),
    b UInt64 CODEC(ZStD(3)),
    c UInt64 CODEC(delta, lz4),
    d UInt64 CODEC(doubledelta, LZ4),
    e UInt64 CODEC(none),
    f UInt64 CODEC(default),
    g UInt64 CODEC(t64('bit'), lz4)
)
ENGINE = MergeTree ORDER BY tuple();

SELECT name, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_codec_case' ORDER BY name;

INSERT INTO t_codec_case SELECT number, number, number, number, number, number, number FROM numbers(1000);
SELECT count(), sum(a + b + c + d + e + f + g) FROM t_codec_case;

SELECT '-- the codec-valued MergeTree settings';

CREATE TABLE t_codec_case_settings (x UInt64)
ENGINE = MergeTree ORDER BY x
SETTINGS default_compression_codec = 'delta, LZ4',
         marks_compression_codec = 'none',
         primary_key_compression_codec = 'DELTA, lz4';

INSERT INTO t_codec_case_settings SELECT number FROM numbers(1000);
SELECT count(), sum(x) FROM t_codec_case_settings;

SELECT '-- another path that takes a codec as a string';

SELECT estimateCompressionRatio('zstd')(number) > 1 FROM numbers(1000);

SELECT '-- `Multiple` is still not nameable directly, in any spelling';

CREATE TABLE t_codec_case_bad (x UInt64 CODEC(multiple)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }
CREATE TABLE t_codec_case_bad (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'multiple'; -- { serverError UNKNOWN_CODEC }

SELECT '-- an unknown codec is still unknown';

CREATE TABLE t_codec_case_bad (x UInt64 CODEC(NoSuchCodec)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }
CREATE TABLE t_codec_case_bad (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'NoSuchCodec'; -- { serverError UNKNOWN_CODEC }

DROP TABLE t_codec_case;
DROP TABLE t_codec_case_settings;
