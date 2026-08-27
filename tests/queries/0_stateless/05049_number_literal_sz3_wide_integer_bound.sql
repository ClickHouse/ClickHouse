-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

-- The error bound of the SZ3 codec is a Float64, but it does not always arrive spelled as one:
-- a large value formats back as plain digits, which re-parse as a wide integer. Any numeric literal
-- has to be accepted, or a table whose metadata carries that spelling can no longer be attached.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_sz3_wide;
CREATE TABLE t_sz3_wide (x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 100000000000000000000))) ENGINE = Memory;
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_sz3_wide';
DROP TABLE t_sz3_wide;

DROP TABLE IF EXISTS t_sz3_exponent;
CREATE TABLE t_sz3_exponent (x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 1e20))) ENGINE = Memory;
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_sz3_exponent';
DROP TABLE t_sz3_exponent;

DROP TABLE IF EXISTS t_sz3_integer;
CREATE TABLE t_sz3_integer (x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 1))) ENGINE = Memory;
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_sz3_integer';
DROP TABLE t_sz3_integer;

DROP TABLE IF EXISTS t_sz3_string;
CREATE TABLE t_sz3_string (x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 'x'))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
DROP TABLE IF EXISTS t_sz3_string;

DROP TABLE IF EXISTS t_sz3_negative;
CREATE TABLE t_sz3_negative (x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', -100000000000000000000))) ENGINE = Memory; -- { serverError ILLEGAL_CODEC_PARAMETER }
DROP TABLE IF EXISTS t_sz3_negative;
