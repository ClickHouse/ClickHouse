-- Tags: no-fasttest
-- no-fasttest: the SZ3 codec needs the sz3 library

DROP TABLE IF EXISTS t_codec_gate_sz3;

SELECT 'rejected with all settings disabled';
CREATE TABLE t_codec_gate_sz3 (x Float64 CODEC(SZ3)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'the dedicated setting of another codec does not enable SZ3';
SET enable_alp_codec = 1;
CREATE TABLE t_codec_gate_sz3 (x Float64 CODEC(SZ3)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
SET enable_alp_codec = 0;

SELECT 'the dedicated setting enables SZ3';
SET enable_sz3_codec = 1;
CREATE TABLE t_codec_gate_sz3 (x Float64 CODEC(SZ3)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_codec_gate_sz3;
SET enable_sz3_codec = 0;
