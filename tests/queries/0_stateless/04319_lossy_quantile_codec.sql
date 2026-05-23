-- Tags: no-fasttest

SET allow_experimental_codecs = 1;

-- Basic roundtrip with Float32
DROP TABLE IF EXISTS t_lossy_quantile_f32;
CREATE TABLE t_lossy_quantile_f32 (val Float32 CODEC(LossyQuantile(4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_f32 SELECT number / 10.0 FROM numbers(1000);
SELECT count() FROM (SELECT val, rowNumberInAllBlocks() AS number FROM t_lossy_quantile_f32) WHERE abs(val - number / 10.0) < 5.0;

DROP TABLE t_lossy_quantile_f32;

-- Basic roundtrip with Float64
DROP TABLE IF EXISTS t_lossy_quantile_f64;
CREATE TABLE t_lossy_quantile_f64 (val Float64 CODEC(LossyQuantile(4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_f64 SELECT number / 10.0 FROM numbers(1000);
SELECT count() FROM (SELECT val, rowNumberInAllBlocks() AS number FROM t_lossy_quantile_f64) WHERE abs(val - number / 10.0) < 5.0;

DROP TABLE t_lossy_quantile_f64;

-- Compression ratio test: K=2 (2 bits) should compress 4x vs raw
DROP TABLE IF EXISTS t_lossy_quantile_ratio;
CREATE TABLE t_lossy_quantile_ratio (val Float32 CODEC(LossyQuantile(2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_ratio SELECT rand(1) / 4294967295.0 FROM numbers(100000);
SELECT
    data_compressed_bytes < data_uncompressed_bytes / 2 AS is_compressed
FROM system.columns
WHERE database = currentDatabase() AND table = 't_lossy_quantile_ratio' AND name = 'val';

DROP TABLE t_lossy_quantile_ratio;

-- All identical values
DROP TABLE IF EXISTS t_lossy_quantile_const;
CREATE TABLE t_lossy_quantile_const (val Float32 CODEC(LossyQuantile(4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_const SELECT 42.0 FROM numbers(100);
SELECT DISTINCT val FROM t_lossy_quantile_const;

DROP TABLE t_lossy_quantile_const;

-- NaN handling
DROP TABLE IF EXISTS t_lossy_quantile_nan;
CREATE TABLE t_lossy_quantile_nan (val Float32 CODEC(LossyQuantile(4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_nan VALUES (1.0)(2.0)(nan)(4.0)(5.0);
SELECT count() FROM t_lossy_quantile_nan WHERE isFinite(val);

DROP TABLE t_lossy_quantile_nan;

-- Inf handling
DROP TABLE IF EXISTS t_lossy_quantile_inf;
CREATE TABLE t_lossy_quantile_inf (val Float32 CODEC(LossyQuantile(4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_inf VALUES (1.0)(2.0)(inf)(-inf)(5.0);
SELECT count() FROM t_lossy_quantile_inf WHERE isFinite(val);

DROP TABLE t_lossy_quantile_inf;

-- Stripe mode: 2 interleaved dimensions
DROP TABLE IF EXISTS t_lossy_quantile_stripe;
CREATE TABLE t_lossy_quantile_stripe (val Float32 CODEC(LossyQuantile(4, 1048576, 2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_stripe SELECT if(number % 2 = 0, number / 100.0, 1000.0 + number / 100.0) FROM numbers(1000);
SELECT
    min(val) < 100 AND max(val) > 900 AS has_range
FROM t_lossy_quantile_stripe;

DROP TABLE t_lossy_quantile_stripe;

-- Parameter validation: bits = 0
SELECT 'Invalid bits = 0:';
CREATE TABLE t_lossy_quantile_err (val Float32 CODEC(LossyQuantile(0))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Parameter validation: bits = 9
SELECT 'Invalid bits = 9:';
CREATE TABLE t_lossy_quantile_err (val Float32 CODEC(LossyQuantile(9))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Parameter validation: group_size = 0
SELECT 'Invalid group_size = 0:';
CREATE TABLE t_lossy_quantile_err (val Float32 CODEC(LossyQuantile(4, 0))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- Parameter validation: no arguments
SELECT 'No arguments:';
CREATE TABLE t_lossy_quantile_err (val Float32 CODEC(LossyQuantile())) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- Various bits values roundtrip
DROP TABLE IF EXISTS t_lossy_quantile_bits;
CREATE TABLE t_lossy_quantile_bits (val Float32 CODEC(LossyQuantile(8))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_bits SELECT sin(number) FROM numbers(1000);
SELECT count() FROM (SELECT val, rowNumberInAllBlocks() AS number FROM t_lossy_quantile_bits) WHERE abs(val - sin(number)) < 0.01;

DROP TABLE t_lossy_quantile_bits;

-- K=1 (extreme compression, 2 centroids)
DROP TABLE IF EXISTS t_lossy_quantile_k1;
CREATE TABLE t_lossy_quantile_k1 (val Float32 CODEC(LossyQuantile(1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lossy_quantile_k1 SELECT number FROM numbers(100);
SELECT count(DISTINCT val) <= 2 AS two_values FROM t_lossy_quantile_k1;

DROP TABLE t_lossy_quantile_k1;

-- Requires allow_experimental_codecs
SET allow_experimental_codecs = 0;
SELECT 'Without experimental codecs:';
CREATE TABLE t_lossy_quantile_exp (val Float32 CODEC(LossyQuantile(4))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
