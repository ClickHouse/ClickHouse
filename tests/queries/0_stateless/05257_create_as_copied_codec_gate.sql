-- `CREATE TABLE ... AS` and `CLONE AS` copy the column codecs of the source table into a new definition,
-- so the copied codecs have to pass the codec gates of the current session, like an explicit column list.

DROP TABLE IF EXISTS t_codec_as_src;
DROP TABLE IF EXISTS t_codec_as_copy;
DROP TABLE IF EXISTS t_codec_as_clone;
DROP TABLE IF EXISTS t_codec_as_suspicious_src;
DROP TABLE IF EXISTS t_codec_as_suspicious_copy;

SET enable_zxc_codec = 1;
CREATE TABLE t_codec_as_src (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_codec_as_src VALUES (1), (2), (3);

SET enable_zxc_codec = 0;
CREATE TABLE t_codec_as_copy AS t_codec_as_src; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_codec_as_clone CLONE AS t_codec_as_src; -- { serverError BAD_ARGUMENTS }

SET enable_zxc_codec = 1;
CREATE TABLE t_codec_as_copy AS t_codec_as_src;
CREATE TABLE t_codec_as_clone CLONE AS t_codec_as_src;

SELECT table, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table IN ('t_codec_as_copy', 't_codec_as_clone')
ORDER BY table;
SELECT sum(x) FROM t_codec_as_clone;

-- Only the gates are re-applied: the suspicious-codec checks already passed when the source table was created.
SET allow_suspicious_codecs = 1;
CREATE TABLE t_codec_as_suspicious_src (x UInt64 CODEC(Delta)) ENGINE = MergeTree ORDER BY x;
SET allow_suspicious_codecs = 0;
CREATE TABLE t_codec_as_suspicious_copy AS t_codec_as_suspicious_src;
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_codec_as_suspicious_copy';

DROP TABLE t_codec_as_src;
DROP TABLE t_codec_as_copy;
DROP TABLE t_codec_as_clone;
DROP TABLE t_codec_as_suspicious_src;
DROP TABLE t_codec_as_suspicious_copy;
