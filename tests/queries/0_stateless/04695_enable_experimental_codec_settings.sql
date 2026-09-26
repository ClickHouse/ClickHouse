DROP TABLE IF EXISTS t_codec_gate;

SELECT 'rejected with all settings disabled';
CREATE TABLE t_codec_gate (x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_codec_gate (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_codec_gate (x Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'the dedicated setting alone enables its codec';
SET enable_alp_codec = 1;
CREATE TABLE t_codec_gate (x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_codec_gate VALUES (1.5), (2.5);
SELECT sum(x) FROM t_codec_gate;
DROP TABLE t_codec_gate;

SELECT 'the dedicated setting of one codec does not enable the others';
CREATE TABLE t_codec_gate (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_codec_gate (x Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
SET enable_alp_codec = 0;

SELECT 'each remaining codec can be enabled with its dedicated setting';
SET enable_zxc_codec = 1;
CREATE TABLE t_codec_gate (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_codec_gate;
SET enable_zxc_codec = 0;

SET enable_quantized_codec = 1;
CREATE TABLE t_codec_gate (x Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_codec_gate;
SET enable_quantized_codec = 0;

SELECT 'the dedicated setting also enables the codec in ALTER';
CREATE TABLE t_codec_gate (x UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_codec_gate ADD COLUMN y Float64 CODEC(ALP); -- { serverError BAD_ARGUMENTS }
SET enable_alp_codec = 1;
ALTER TABLE t_codec_gate ADD COLUMN y Float64 CODEC(ALP);
SET enable_alp_codec = 0;
ALTER TABLE t_codec_gate MODIFY COLUMN y Float64 CODEC(ALP); -- { serverError BAD_ARGUMENTS }
SET enable_alp_codec = 1;
ALTER TABLE t_codec_gate MODIFY COLUMN y Float64 CODEC(ALP);
SET enable_alp_codec = 0;
DROP TABLE t_codec_gate;

SELECT 'the dedicated setting also enables the codec in the codec-valued MergeTree settings';
CREATE TABLE t_codec_gate (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
SET enable_zxc_codec = 1;
CREATE TABLE t_codec_gate (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC';
SET enable_zxc_codec = 0;
DROP TABLE t_codec_gate;

SELECT 'the dedicated setting also enables the codec in ALTER MODIFY SETTING';
CREATE TABLE t_codec_gate (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_codec_gate MODIFY SETTING default_compression_codec = 'ZXC'; -- { serverError BAD_ARGUMENTS }
SET enable_zxc_codec = 1;
ALTER TABLE t_codec_gate MODIFY SETTING default_compression_codec = 'ZXC';
SET enable_zxc_codec = 0;
DROP TABLE t_codec_gate;

SELECT 'the dedicated setting also enables the codec in TTL RECOMPRESS';
CREATE TABLE t_codec_gate (d Date, x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_codec_gate MODIFY TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }
SET enable_zxc_codec = 1;
ALTER TABLE t_codec_gate MODIFY TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZXC);
SET enable_zxc_codec = 0;
DROP TABLE t_codec_gate;
