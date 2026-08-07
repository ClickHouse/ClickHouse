-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: a codec chain long enough to overflow a 32-bit reserve needs a second or two.
-- Tag no-parallel: the 104-stage arm below allocates just under 4 GiB against a per-user memory
-- limit that concurrent copies of this test share, and that SET max_memory_usage = 0 does not lift.

-- CompressionCodecMultiple compounded each stage's reserved size in a UInt32 without checking for
-- overflow. Every codec reserves more than its input, so a long enough chain wrapped the reserve
-- below its own input, and the destination buffer was then sized from the wrapped number while the
-- real payload kept growing: an out-of-bounds heap write, and a server crash. Such a chain is
-- rejected now. A chain of more than 255 codecs is rejected too: the codec count is stored in a
-- single byte, so a longer chain used to write a part that could not be read back.

SET allow_suspicious_codecs = 1;
-- The 104-stage chain below reserves just under 4 GiB for one block, which is what this test is
-- about, and that is most of the per-query limit CI's default profile sets. This test measures a
-- size bound, not memory consumption, so lift the limit rather than tune it.
SET max_memory_usage = 0;

DROP TABLE IF EXISTS t_codec_chain_overflow;
DROP TABLE IF EXISTS t_codec_chain_fpc_max;
DROP TABLE IF EXISTS t_codec_chain_fpc_over;
DROP TABLE IF EXISTS t_codec_chain_gorilla_max;
DROP TABLE IF EXISTS t_codec_chain_gorilla_over;
DROP TABLE IF EXISTS t_codec_chain_dd_max;
DROP TABLE IF EXISTS t_codec_chain_dd_over;
DROP TABLE IF EXISTS t_codec_chain_short;
DROP TABLE IF EXISTS t_codec_chain_255;
DROP TABLE IF EXISTS t_codec_chain_256;

-- 150 FPC stages. Two rows are enough, and before the fix this crashed the server with a SIGSEGV.
-- The block size sets the stage the reserve wraps at, later for smaller blocks: with these 2 rows
-- it wraps at stage 148, the latest any block size can, so do not shorten the chain.
CREATE TABLE t_codec_chain_overflow (x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_codec_chain_overflow SELECT number / 7 FROM numbers(2); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_overflow;

-- 104 FPC stages: the longest chain whose reserve still fits. It must keep working and round-trip.
-- The stage the reserve wraps at is a function of the compressed block size, so the three tables
-- below pin everything that sets it: 5000 Float32 then form one 20000-byte block, wrapping at
-- stage 105. A smaller block wraps later, which would make the next arm accept its chain.
CREATE TABLE t_codec_chain_fpc_max (i UInt32, x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000;
INSERT INTO t_codec_chain_fpc_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_fpc_max;

-- One stage more, on the same fixture, is the first chain that is rejected.
CREATE TABLE t_codec_chain_fpc_over (i UInt32, x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000;
INSERT INTO t_codec_chain_fpc_over SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_fpc_over;

-- Gorilla reserves far more per stage than FPC, so its chains are much shorter. 29 stages is the
-- longest that still fits; it must keep working too.
CREATE TABLE t_codec_chain_gorilla_max (i UInt32, x Float32 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000;
INSERT INTO t_codec_chain_gorilla_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_gorilla_max;

-- The reserve is compounded the same way whatever the codec, so the boundary below is Gorilla's own
-- and one stage past it is rejected, as for FPC above. The three arms that follow pin the sibling
-- formulas, so that rejection cannot silently narrow to FPC alone.
CREATE TABLE t_codec_chain_gorilla_over (i UInt32, x Float32 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000;
INSERT INTO t_codec_chain_gorilla_over SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_gorilla_over;

-- DoubleDelta reserves more per stage still: 13 is the longest chain that fits, 14 is rejected.
CREATE TABLE t_codec_chain_dd_max (i UInt32, x Float32 CODEC(DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000;
INSERT INTO t_codec_chain_dd_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_dd_max;

CREATE TABLE t_codec_chain_dd_over (i UInt32, x Float32 CODEC(DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000;
INSERT INTO t_codec_chain_dd_over SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_dd_over;

-- 255 codecs is the longest chain the one-byte count can describe. Delta does not expand, so the
-- reserve stays small and only the count matters here.
CREATE TABLE t_codec_chain_255 (i UInt32, x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_255 SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_255;

-- One codec more used to write a part that could not be read back.
CREATE TABLE t_codec_chain_256 (i UInt32, x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_256 SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_256;

-- An ordinary short chain is unaffected.
CREATE TABLE t_codec_chain_short (i UInt32, x Float32 CODEC(Delta, ZSTD)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_short SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_short;

DROP TABLE t_codec_chain_overflow;
DROP TABLE t_codec_chain_fpc_max;
DROP TABLE t_codec_chain_fpc_over;
DROP TABLE t_codec_chain_gorilla_max;
DROP TABLE t_codec_chain_gorilla_over;
DROP TABLE t_codec_chain_dd_max;
DROP TABLE t_codec_chain_dd_over;
DROP TABLE t_codec_chain_short;
DROP TABLE t_codec_chain_255;
DROP TABLE t_codec_chain_256;
