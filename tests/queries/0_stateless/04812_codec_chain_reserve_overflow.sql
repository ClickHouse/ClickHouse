-- Tags: no-fasttest
-- Tag no-fasttest: a codec chain long enough to overflow a 32-bit reserve needs a second or two.

-- CompressionCodecMultiple compounded each stage's reserved size in a UInt32 without checking for
-- overflow. Every codec reserves more than its input, so a long enough chain wrapped the reserve
-- below its own input, and the destination buffer was then sized from the wrapped number while the
-- real payload kept growing: an out-of-bounds heap write, and a server crash. Such a chain is
-- rejected now.

SET allow_suspicious_codecs = 1;

DROP TABLE IF EXISTS t_codec_chain_overflow;
DROP TABLE IF EXISTS t_codec_chain_fpc_max;
DROP TABLE IF EXISTS t_codec_chain_gorilla_max;
DROP TABLE IF EXISTS t_codec_chain_short;

-- 150 FPC stages. Two rows are enough, and before the fix this crashed the server with a SIGSEGV.
-- The block size sets the stage the reserve wraps at, later for smaller blocks: with these 2 rows
-- it wraps at stage 148, the latest any block size can, so do not shorten the chain.
CREATE TABLE t_codec_chain_overflow (x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_codec_chain_overflow SELECT number / 7 FROM numbers(2); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_overflow;

-- 104 FPC stages: the longest chain whose reserve still fits. It must keep working and round-trip.
CREATE TABLE t_codec_chain_fpc_max (i UInt32, x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_fpc_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_fpc_max;

-- Gorilla reserves far more per stage than FPC, so its chains are much shorter. 29 stages is the
-- longest that still fits; it must keep working too.
CREATE TABLE t_codec_chain_gorilla_max (i UInt32, x Float32 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_gorilla_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_gorilla_max;

-- An ordinary short chain is unaffected.
CREATE TABLE t_codec_chain_short (i UInt32, x Float32 CODEC(Delta, ZSTD)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_short SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_short;

DROP TABLE t_codec_chain_overflow;
DROP TABLE t_codec_chain_fpc_max;
DROP TABLE t_codec_chain_gorilla_max;
DROP TABLE t_codec_chain_short;
