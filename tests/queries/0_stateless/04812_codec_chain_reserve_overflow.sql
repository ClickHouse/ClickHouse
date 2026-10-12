-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: a codec chain long enough to overflow a 32-bit reserve needs a second or two.
-- Tag no-parallel: the 104-stage arm below allocates just under 4 GiB against a per-user memory
-- limit that concurrent copies of this test share, and that SET max_memory_usage = 0 does not lift.
-- Random settings limits: min_columns_to_activate_adaptive_write_buffer=(3, None)

-- CompressionCodecMultiple compounded each stage's reserved size in a UInt32 without checking for
-- overflow. Every codec reserves more than its input, so a long enough chain wrapped the reserve
-- below its own input, and the destination buffer was then sized from the wrapped number while the
-- real payload kept growing: an out-of-bounds heap write, and a server crash. Such a chain is
-- rejected when the table is created now. A chain of more than 255 codecs is rejected there too:
-- the codec count is stored in a single byte, so a longer chain used to write an unreadable part.

-- The 104-stage chain below reserves just under 4 GiB for one block, which is what this test is
-- about, and that is most of the per-query limit CI's default profile sets. This test measures a
-- size bound, not memory consumption, so lift the limit rather than tune it.
SET max_memory_usage = 0;

DROP TABLE IF EXISTS t_codec_chain_overflow;
DROP TABLE IF EXISTS t_codec_chain_fpc_max;
DROP TABLE IF EXISTS t_codec_chain_fpc_over;
DROP TABLE IF EXISTS t_codec_chain_gorilla_wide;
DROP TABLE IF EXISTS t_codec_chain_gorilla_max;
DROP TABLE IF EXISTS t_codec_chain_gorilla_over;
DROP TABLE IF EXISTS t_codec_chain_gorilla64_max;
DROP TABLE IF EXISTS t_codec_chain_gorilla64_over;
DROP TABLE IF EXISTS t_codec_chain_dd_max;
DROP TABLE IF EXISTS t_codec_chain_dd_over;
DROP TABLE IF EXISTS t_codec_chain_short;
DROP TABLE IF EXISTS t_codec_chain_255;
DROP TABLE IF EXISTS t_codec_chain_256;
DROP TABLE IF EXISTS t_codec_chain_256_meta;
DROP TABLE IF EXISTS t_codec_chain_meta_alter;
DROP TABLE IF EXISTS t_codec_chain_tuple_mixed;
DROP TABLE IF EXISTS t_codec_chain_tuple_wide;

-- 150 FPC stages. Compressing a block with this chain is impossible whatever the block size, so
-- the chain is rejected when the table is created. Before the fix the table was created and the
-- first write crashed the server with an out-of-bounds heap write.
CREATE TABLE t_codec_chain_overflow (x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- 104 FPC stages: the longest chain whose reserve still fits. It must keep working and round-trip.
-- The stage the reserve wraps at is a function of the compressed block size, so the three tables
-- below pin everything that sets it: 5000 Float32 then form one 20000-byte block, wrapping at
-- stage 105. A smaller block wraps later, which would make the next arm accept its chain.
CREATE TABLE t_codec_chain_fpc_max (i UInt32, x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_fpc_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_fpc_max;

-- One stage more, on the same fixture, is the first chain that is rejected.
CREATE TABLE t_codec_chain_fpc_over (i UInt32, x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_fpc_over SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_fpc_over;

-- Gorilla's reserve per item is a function of the column's width, so a chain of it has a boundary
-- per width. Both boundaries are pinned below, because whichever width a process compresses first
-- is the one that used to fix the reserve for every other width, and a test cannot choose that.
CREATE TABLE t_codec_chain_gorilla_wide (d Float64 CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_codec_chain_gorilla_wide SELECT number / 7 FROM numbers(4);
SELECT count() FROM t_codec_chain_gorilla_wide;

-- Gorilla reserves far more per stage than FPC, so its chains are much shorter. 29 stages is the
-- longest that still fits; it must keep working too.
CREATE TABLE t_codec_chain_gorilla_max (i UInt32, x Float32 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_gorilla_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_gorilla_max;

-- The reserve is compounded the same way whatever the codec, so the boundary below is Gorilla's own
-- and one stage past it is rejected, as for FPC above. The three arms that follow pin the sibling
-- formulas, so that rejection cannot silently narrow to FPC alone.
CREATE TABLE t_codec_chain_gorilla_over (i UInt32, x Float32 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_gorilla_over SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_gorilla_over;

-- The same boundary for a Float64 Gorilla column: 48 stages fit, 49 do not. A chain of one width
-- must be judged on its own width, so these two keep their verdicts however the Float32 arms above
-- left the process.
CREATE TABLE t_codec_chain_gorilla64_max (i UInt32, d Float64 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_gorilla64_max SELECT number, number / 7 FROM numbers(2500);
SELECT count(), countIf(d != i / 7) FROM t_codec_chain_gorilla64_max;

CREATE TABLE t_codec_chain_gorilla64_over (i UInt32, d Float64 CODEC(Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla,Gorilla)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_gorilla64_over SELECT number, number / 7 FROM numbers(2500); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_gorilla64_over;

-- DoubleDelta reserves more per stage still: 13 is the longest chain that fits, 14 is rejected.
CREATE TABLE t_codec_chain_dd_max (i UInt32, x Float32 CODEC(DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_dd_max SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_dd_max;

CREATE TABLE t_codec_chain_dd_over (i UInt32, x Float32 CODEC(DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta,DoubleDelta)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0,
             min_compress_block_size = 20000, max_compress_block_size = 20000,
             ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_codec_chain_dd_over SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_dd_over;

-- 255 codecs is the longest chain the one-byte count can describe. Delta does not expand, so the
-- reserve stays small and only the count matters here.
CREATE TABLE t_codec_chain_255 (i UInt32, x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_255 SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_255;

-- One codec more used to write a part that could not be read back.
CREATE TABLE t_codec_chain_256 (i UInt32, x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)) ENGINE = MergeTree ORDER BY i SETTINGS allow_suspicious_codecs = 1;
INSERT INTO t_codec_chain_256 SELECT number, number / 7 FROM numbers(5000); -- { serverError CANNOT_COMPRESS }
SELECT count() FROM t_codec_chain_256;

-- A chain the one-byte count cannot describe is rejected when the metadata is created, so that it
-- cannot be persisted and replicated as a codec no replica can ever write with. This check is a
-- sanity check, so the arms above, which set allow_suspicious_codecs, still reach the write path.
CREATE TABLE t_codec_chain_256_meta (i UInt32, x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)) ENGINE = MergeTree ORDER BY i
    SETTINGS allow_suspicious_codecs = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_codec_chain_meta_alter (i UInt32, x Float32, w Float64) ENGINE = MergeTree ORDER BY i;
ALTER TABLE t_codec_chain_meta_alter MODIFY COLUMN x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)
    SETTINGS allow_suspicious_codecs = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_codec_chain_meta_alter ADD COLUMN y Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)
    SETTINGS allow_suspicious_codecs = 0; -- { serverError BAD_ARGUMENTS }

-- The longest chain the count can describe is still accepted there.
ALTER TABLE t_codec_chain_meta_alter MODIFY COLUMN x Float32 CODEC(Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,Delta,ZSTD)
    SETTINGS allow_suspicious_codecs = 0;
SELECT count() FROM t_codec_chain_meta_alter;

-- The reserve rule applies to ALTER as well, for both forms that carry a codec.
ALTER TABLE t_codec_chain_meta_alter MODIFY COLUMN x Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_codec_chain_meta_alter ADD COLUMN z Float32 CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC); -- { serverError BAD_ARGUMENTS }

-- A codec whose reserve depends on the column width resolves per substream, so a heterogeneous
-- column has one chain per substream and each of them must fit. Either element order is rejected
-- because the Float32 substream alone cannot compress a block.
CREATE TABLE t_codec_chain_tuple_mixed (x Tuple(Float32, Float64) CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_codec_chain_tuple_mixed (x Tuple(Float64, Float32) CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The same chain on a column all of whose substreams are 8 bytes wide reserves far less and stays
-- accepted, so the check above is not a blanket rejection of the chain length.
CREATE TABLE t_codec_chain_tuple_wide (x Tuple(Float64, Float64) CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC)) ENGINE = MergeTree ORDER BY tuple();

-- MODIFY COLUMN may omit the type, and the codec then resolves against the type the column
-- already has, so the same chain is judged by that width and not by the null-type default.
ALTER TABLE t_codec_chain_meta_alter MODIFY COLUMN x CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_codec_chain_meta_alter MODIFY COLUMN w CODEC(FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC,FPC);
SELECT count() FROM t_codec_chain_meta_alter;

-- An ordinary short chain is unaffected.
CREATE TABLE t_codec_chain_short (i UInt32, x Float32 CODEC(Delta, ZSTD)) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_codec_chain_short SELECT number, number / 7 FROM numbers(5000);
SELECT count(), countIf(x != toFloat32(i / 7)) FROM t_codec_chain_short;

DROP TABLE t_codec_chain_fpc_max;
DROP TABLE t_codec_chain_fpc_over;
DROP TABLE t_codec_chain_gorilla_wide;
DROP TABLE t_codec_chain_gorilla_max;
DROP TABLE t_codec_chain_gorilla_over;
DROP TABLE t_codec_chain_gorilla64_max;
DROP TABLE t_codec_chain_gorilla64_over;
DROP TABLE t_codec_chain_dd_max;
DROP TABLE t_codec_chain_dd_over;
DROP TABLE t_codec_chain_short;
DROP TABLE t_codec_chain_255;
DROP TABLE t_codec_chain_256;
DROP TABLE t_codec_chain_meta_alter;
DROP TABLE t_codec_chain_tuple_wide;
