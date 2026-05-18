-- Test MD5 multi-buffer SIMD implementation for correctness.
-- Verifies that the SIMD batched path produces byte-identical results
-- to the RFC 1321 reference vectors across various input sizes and batch boundaries.

-- RFC 1321 test vectors
SELECT hex(MD5(''));
SELECT hex(MD5('a'));
SELECT hex(MD5('abc'));
SELECT hex(MD5('message digest'));
SELECT hex(MD5('abcdefghijklmnopqrstuvwxyz'));
SELECT hex(MD5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'));
SELECT hex(MD5('12345678901234567890123456789012345678901234567890123456789012345678901234567890'));

-- Boundary sizes for MD5 padding (1 block vs 2 blocks threshold is at 55/56 bytes)
SELECT hex(MD5(repeat('x', 55)));  -- exactly fills 1 block after padding
SELECT hex(MD5(repeat('x', 56)));  -- spills to 2 blocks
SELECT hex(MD5(repeat('x', 64)));  -- exactly 1 full block + padding block
SELECT hex(MD5(repeat('x', 128))); -- 2 full blocks + padding block

-- Batch boundary tests: exercise partial batches at each dispatch level.
-- Scalar path: 2 lanes x 2 groups = 4 digests per iteration.
-- 3 rows (partial scalar batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(3);
-- 4 rows (exact scalar batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(4);
-- 5 rows (scalar batch + 1 overflow)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(5);
-- AVX2 = 8 lanes x 2 groups = 16 digests, AVX-512 = 16 lanes x 2 groups = 32 digests.
-- 7 rows (partial AVX2 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(7);
-- 8 rows (exact AVX2 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(8);
-- 9 rows (AVX2 batch + 1 overflow)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(9);
-- 15 rows (partial AVX-512 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(15);
-- 16 rows (exact AVX-512 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(16);
-- 17 rows (AVX-512 batch + 1 overflow)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(17);
-- Larger batch to exercise multiple full SIMD iterations
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(100);

-- FixedString input
SELECT hex(MD5(toFixedString('hello', 5)));
SELECT hex(MD5(toFixedString('abc', 10)));

-- Variable-length strings with very different lengths in the same batch
-- (tests lane divergence in multi-buffer processing)
SELECT hex(MD5(s)) FROM
(
    SELECT arrayJoin(['', 'a', repeat('b', 55), repeat('c', 56), repeat('d', 100), repeat('e', 200)]) AS s
)
ORDER BY length(s);

-- IPv6 input
SELECT hex(MD5(toIPv6('::1')));

-- Single row (tests batch size = 1)
SELECT hex(MD5('single'));
