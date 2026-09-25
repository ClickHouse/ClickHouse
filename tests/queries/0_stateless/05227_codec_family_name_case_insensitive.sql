-- A codec family name is matched case-insensitively, the way the `CODEC` keyword itself is. Without that, a codec whose
-- canonical name is mixed-case (`Delta`, `DoubleDelta`, `Gorilla`, ...) could not be named at all in the
-- codec-valued `MergeTree` settings or in the `<compression>` server config, because those paths used to
-- upper-case the whole codec expression before parsing it.

SET enable_alp_codec = 1;

DROP TABLE IF EXISTS t_codec_case;
DROP TABLE IF EXISTS t_codec_case_settings;
DROP TABLE IF EXISTS t_codec_case_marks;
DROP TABLE IF EXISTS t_codec_case_args;
DROP TABLE IF EXISTS t_codec_case_default_alp;

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

SELECT '-- the marks codec is parsed as written on every write path: compact and wide parts, and text index';

CREATE TABLE t_codec_case_marks (x UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, marks_compression_codec = 'Delta, lz4', index_granularity = 8;

INSERT INTO t_codec_case_marks SELECT number, 'word' || toString(number) FROM numbers(100);
SELECT count(), sum(x) FROM t_codec_case_marks WHERE hasToken(s, 'word42');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_codec_case_marks' AND active;

SELECT '-- a keyword argument of a codec is case-insensitive too, so a setting keeps its previously valid spelling';

CREATE TABLE t_codec_case_args
(
    a Float64 CODEC(alp(std)),
    b Float64 CODEC(ALP(auto)),
    c UInt64 CODEC(T64('BIT')),
    d Tuple(UInt32, UInt64) CODEC(Delta, default)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SELECT name, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_codec_case_args' ORDER BY name;

INSERT INTO t_codec_case_args SELECT number / 8, number / 4, number, (number, number) FROM numbers(1000);
SELECT count(), sum(a), sum(b), sum(c), sum(d.2) FROM t_codec_case_args;

-- The part's default codec also compresses the packed statistics file, which a type-dependent codec such as
-- `ALP` cannot do, so automatic statistics are turned off here; that is a pre-existing limitation of
-- type-dependent codecs in `default_compression_codec`, not a property of the spelling.
CREATE TABLE t_codec_case_default_alp (f Float64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'ALP(std)', auto_statistics_types = '';

INSERT INTO t_codec_case_default_alp SELECT number / 8 FROM numbers(1000);
SELECT count(), sum(f) FROM t_codec_case_default_alp;
SELECT default_compression_codec FROM system.parts WHERE database = currentDatabase() AND table = 't_codec_case_default_alp' AND active;

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
DROP TABLE t_codec_case_marks;
DROP TABLE t_codec_case_args;
DROP TABLE t_codec_case_default_alp;
