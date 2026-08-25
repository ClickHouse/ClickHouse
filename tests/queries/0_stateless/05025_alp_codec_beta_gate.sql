DROP TABLE IF EXISTS t_alp_beta;

SELECT 'a beta codec is gated by default';
CREATE TABLE t_alp_beta (x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'the umbrella does not enable a beta codec';
SET allow_experimental_codecs = 1;
CREATE TABLE t_alp_beta (x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
SET allow_experimental_codecs = 0;

SELECT 'the dedicated setting enables the beta codec';
SET enable_alp_codec = 1;
CREATE TABLE t_alp_beta (x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alp_beta VALUES (1.5), (2.5);
SELECT sum(x) FROM t_alp_beta;
DROP TABLE t_alp_beta;
SET enable_alp_codec = 0;

SELECT 'the umbrella still enables experimental-tier codecs';
SET allow_experimental_codecs = 1;
CREATE TABLE t_zxc_experimental (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_zxc_experimental;
SET allow_experimental_codecs = 0;

SELECT 'system.codecs reports the tier';
SELECT name, tier, is_experimental FROM system.codecs WHERE name IN ('ALP', 'ZXC', 'LZ4') ORDER BY name;
