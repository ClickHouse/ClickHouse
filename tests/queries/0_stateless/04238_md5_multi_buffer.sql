-- Tags: no-openssl-fips
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

-- Batch boundary tests with various row counts.
-- Note: ImplementationSelector only benchmarks implementations for input_rows_count > 1000,
-- so these small batches run the default (scalar) path via the selector.
-- The gtest (gtest_md5.cpp) provides deterministic per-arch SIMD coverage.
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(3);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(4);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(5);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(7);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(8);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(9);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(15);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(16);
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(17);
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

-- Long string: exercises many full blocks through the main loop (100000 bytes = 1562.5 blocks)
SELECT hex(MD5(repeat('x', 100000)));

-- Single row (tests batch size = 1)
SELECT hex(MD5('single'));

-- Large batch: crosses the ImplementationSelector threshold (> 1000 rows),
-- enabling it to benchmark and potentially select a SIMD implementation.
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(10000);
