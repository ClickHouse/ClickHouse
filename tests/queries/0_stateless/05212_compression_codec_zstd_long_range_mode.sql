-- Tests the two-argument ZSTD codec form ZSTD(level, window_log) that enables
-- long-distance matching.  The window_log argument activates a separate
-- constructor (CompressionCodecZSTD.cpp:66-71) with enable_long_range=true,
-- the ZSTD_CCtx_setParameter long-range block during compression
-- (CompressionCodecZSTD.cpp:42-46), the getCodecDesc branch that emits the
-- two-argument AST representation (CompressionCodecZSTD.cpp:83-84), and the
-- factory parser's two-argument path including the window_log bounds check
-- (CompressionCodecZSTD.cpp:111-131).
-- All CI coverage of ZSTD previously came from ZSTD(level) (one argument),
-- because the only two-argument test (01622) is tagged 'long' and is excluded
-- from the standard coverage runs.

-- Regression: if the long-range branch silently reverted to single-argument
-- behaviour, the table would still return results but the long-range
-- constructor and compression context parameters would be untested.

DROP TABLE IF EXISTS t_zstd_lr;

-- Two-argument form: level=3, window_log=24 (valid range is [10, 31]).
CREATE TABLE t_zstd_lr (n UInt32, s String CODEC(ZSTD(3, 24)))
ENGINE = MergeTree ORDER BY n;

INSERT INTO t_zstd_lr SELECT number, repeat('x', 20) FROM numbers(50);
SELECT count(), sum(n) FROM t_zstd_lr;

-- window_log = 10 (lower bound).
DROP TABLE IF EXISTS t_zstd_lr_lo;
CREATE TABLE t_zstd_lr_lo (n UInt32 CODEC(ZSTD(1, 10)))
ENGINE = MergeTree ORDER BY n;
INSERT INTO t_zstd_lr_lo VALUES (42);
SELECT n FROM t_zstd_lr_lo;
DROP TABLE t_zstd_lr_lo;

-- window_log out of bounds → ILLEGAL_CODEC_PARAMETER.
CREATE TABLE t_bad (x UInt32 CODEC(ZSTD(1, 999))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }

DROP TABLE t_zstd_lr;
