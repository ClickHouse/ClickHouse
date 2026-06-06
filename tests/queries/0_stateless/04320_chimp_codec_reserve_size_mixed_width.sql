-- Regression test for the Chimp codec output-buffer reservation.
-- `getCompressedDataSize` must compute the per-item bit width from `data_bytes_size`
-- on every call. Previously it cached the width in `static` locals, so the first
-- width seen in the process (e.g. Chimp(4)) was reused for every later width
-- (e.g. Chimp(8)), under-reserving the buffer and triggering a BitWriter
-- end-of-buffer exception on large, poorly compressible Float64 blocks.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_chimp_4;
DROP TABLE IF EXISTS t_chimp_8;

-- Use the 4-byte width first.
CREATE TABLE t_chimp_4 (x Float32 CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chimp_4 SELECT number / 3 FROM numbers(1000);
SELECT count() FROM t_chimp_4;

-- Then a large 8-byte block of poorly compressible values, which needs the full
-- 8-byte per-item width. With the bug this INSERT failed to compress.
CREATE TABLE t_chimp_8 (x Float64 CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chimp_8 SELECT sin(number * number * number) * number FROM numbers(200000);
SELECT count() FROM t_chimp_8;

DROP TABLE t_chimp_4;
DROP TABLE t_chimp_8;
