-- A filter is counted 64 bytes at a time with a byte-at-a-time tail, so a count must not
-- depend on whether the block length is a multiple of 64. Partial selectivity is required:
-- an all-pass filter cannot tell a correct block mask from a broken one.
-- max_block_size pins the filter length so each query lands on a chosen boundary.

SELECT '-- countBytesInFilter, one block length per row';
SELECT countIf(number % 3 = 1) FROM numbers(0)   SETTINGS max_block_size = 1;
SELECT countIf(number % 3 = 1) FROM numbers(1)   SETTINGS max_block_size = 1;
SELECT countIf(number % 3 = 1) FROM numbers(63)  SETTINGS max_block_size = 63;
SELECT countIf(number % 3 = 1) FROM numbers(64)  SETTINGS max_block_size = 64;
SELECT countIf(number % 3 = 1) FROM numbers(65)  SETTINGS max_block_size = 65;
SELECT countIf(number % 3 = 1) FROM numbers(127) SETTINGS max_block_size = 127;
SELECT countIf(number % 3 = 1) FROM numbers(128) SETTINGS max_block_size = 128;
SELECT countIf(number % 3 = 1) FROM numbers(129) SETTINGS max_block_size = 129;
SELECT countIf(number % 3 = 1) FROM numbers(255) SETTINGS max_block_size = 255;
SELECT countIf(number % 3 = 1) FROM numbers(256) SETTINGS max_block_size = 256;
SELECT countIf(number % 3 = 1) FROM numbers(257) SETTINGS max_block_size = 257;

SELECT '-- countBytesInFilter reached with the output column optimised away';
SELECT count() FROM numbers(63)  WHERE number % 3 = 1 SETTINGS max_block_size = 63;
SELECT count() FROM numbers(64)  WHERE number % 3 = 1 SETTINGS max_block_size = 64;
SELECT count() FROM numbers(65)  WHERE number % 3 = 1 SETTINGS max_block_size = 65;
SELECT count() FROM numbers(128) WHERE number % 3 = 1 SETTINGS max_block_size = 128;
SELECT count() FROM numbers(193) WHERE number % 3 = 1 SETTINGS max_block_size = 193;

-- countIfOrNull is the shape that reaches countBytesInFilterWithNull: it needs a null map
-- forwarded together with a filter argument. A plain countIf over a Nullable column folds the
-- null map into its own filter and never calls it.
SELECT '-- countBytesInFilterWithNull';
SELECT countIfOrNull(if(number % 5 = 0, NULL, number), number % 3 = 1) FROM numbers(63)  SETTINGS max_block_size = 63;
SELECT countIfOrNull(if(number % 5 = 0, NULL, number), number % 3 = 1) FROM numbers(64)  SETTINGS max_block_size = 64;
SELECT countIfOrNull(if(number % 5 = 0, NULL, number), number % 3 = 1) FROM numbers(65)  SETTINGS max_block_size = 65;
SELECT countIfOrNull(if(number % 5 = 0, NULL, number), number % 3 = 1) FROM numbers(128) SETTINGS max_block_size = 128;
SELECT countIfOrNull(if(number % 5 = 0, NULL, number), number % 3 = 1) FROM numbers(129) SETTINGS max_block_size = 129;
SELECT countIfOrNull(if(number % 5 = 0, NULL, number), number % 3 = 1) FROM numbers(257) SETTINGS max_block_size = 257;

SELECT '-- all-zero, all-one and a single set byte in the second block';
SELECT count() FROM numbers(128) WHERE number > 1000 SETTINGS max_block_size = 128;
SELECT count() FROM numbers(128) WHERE number < 1000 SETTINGS max_block_size = 128;
SELECT count() FROM numbers(128) WHERE number = 100  SETTINGS max_block_size = 128;

SELECT '-- a filter spanning many blocks must agree with the sum of its parts';
SELECT countIf(number % 7 = 3) = (SELECT count() FROM numbers(100000) WHERE number % 7 = 3)
FROM numbers(100000) SETTINGS max_block_size = 100000;

-- An -If condition column is forwarded as a raw UInt8, so a byte outside 0/1 reaches these loops.
-- Each condition byte below keeps a set bit outside the null byte, so `filter & ~null` stays
-- non-zero on a NULL row while `filter != 0 && null == 0` is false: the two disagree there.
SELECT '-- non-binary UInt8 condition, block-size independent';
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 63;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 64;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 100;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(255)) FROM numbers(63) SETTINGS max_block_size = 63;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(128)) FROM numbers(129) SETTINGS max_block_size = 129;
