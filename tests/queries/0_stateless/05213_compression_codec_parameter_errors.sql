-- Tests argument-validation error branches in several compression codecs that
-- had zero CI coverage.
--
-- COVERAGE TARGETS
--   src/Compression/CompressionCodecGCD.cpp:321-322
--     GCD codec takes no parameters; passing one is ILLEGAL_SYNTAX_FOR_CODEC_TYPE.
--
--   src/Compression/CompressionFactory.cpp:240-241
--     The "Multiple" codec is used internally for chained codecs and cannot be
--     addressed by name in DDL; doing so is UNKNOWN_CODEC.
--
--   src/Compression/CompressionCodecQuantized.cpp:72-74
--     Quantized requires exactly 2-4 arguments; fewer than 2 is ILLEGAL_SYNTAX_FOR_CODEC_TYPE.
--
--   src/Compression/CompressionCodecQuantized.cpp:77-78
--     The first argument (method) must be a String literal, not an integer; ILLEGAL_CODEC_PARAMETER.
--
--   src/Compression/CompressionCodecQuantized.cpp:81-82
--     The second argument (dimensions) must be an unsigned integer, not a string; ILLEGAL_CODEC_PARAMETER.
--
-- None of these error paths are reached by any existing stateless test because
-- valid codec usage never exercises the wrong-argument branches.

-- GCD codec with parameter → ILLEGAL_SYNTAX_FOR_CODEC_TYPE.
CREATE TABLE t_gcd_param (x UInt32 CODEC(GCD(1))) ENGINE = Memory; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- "Multiple" codec used directly → UNKNOWN_CODEC.
CREATE TABLE t_multiple (x UInt32 CODEC(Multiple)) ENGINE = Memory; -- { serverError UNKNOWN_CODEC }

-- Quantized: fewer than 2 arguments → ILLEGAL_SYNTAX_FOR_CODEC_TYPE.
CREATE TABLE t_q1 (x Float32 CODEC(Quantized('dct'))) ENGINE = Memory SETTINGS enable_quantized_codec = 1; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- Quantized: first argument is integer, not string → ILLEGAL_CODEC_PARAMETER.
CREATE TABLE t_q2 (x Float32 CODEC(Quantized(42, 3))) ENGINE = Memory SETTINGS enable_quantized_codec = 1; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Quantized: second argument is string, not unsigned integer → ILLEGAL_CODEC_PARAMETER.
CREATE TABLE t_q3 (x Float32 CODEC(Quantized('dct', 'bad'))) ENGINE = Memory SETTINGS enable_quantized_codec = 1; -- { serverError ILLEGAL_CODEC_PARAMETER }
