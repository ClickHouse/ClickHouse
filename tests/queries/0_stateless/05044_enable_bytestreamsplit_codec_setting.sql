DROP TABLE IF EXISTS t_codec_gate_bss;

SELECT 'rejected with all settings disabled';
CREATE TABLE t_codec_gate_bss (x Float64 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'the dedicated setting of another codec does not enable ByteStreamSplit';
SET enable_alp_codec = 1;
CREATE TABLE t_codec_gate_bss (x Float64 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
SET enable_alp_codec = 0;

SELECT 'the dedicated setting enables ByteStreamSplit';
SET enable_bytestreamsplit_codec = 1;
CREATE TABLE t_codec_gate_bss (x Float64 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_codec_gate_bss VALUES (1.5), (2.5);
SELECT sum(x) FROM t_codec_gate_bss;
DROP TABLE t_codec_gate_bss;
SET enable_bytestreamsplit_codec = 0;

SELECT 'allow_experimental_codecs still enables it';
SET allow_experimental_codecs = 1;
CREATE TABLE t_codec_gate_bss (x Float64 CODEC(ByteStreamSplit, LZ4)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_codec_gate_bss;
