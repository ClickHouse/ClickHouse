-- Regression test for countBytesInFilterWithNull with non-boolean UInt8 conditions.
-- Before the fix, the scalar tail overcounted when (filter_byte != 0 && null_byte != 0)
-- because (*pos & ~*pos2) != 0 evaluated to true (e.g. 2 & ~1 == 2 != 0).
-- max_block_size = 65 forces a 64-byte block followed by a 1-row scalar tail.

SELECT countIf(CAST(if(number % 2 = 0, NULL, 1) AS Nullable(UInt8)), toUInt8(2)) FROM numbers(65) SETTINGS max_block_size = 65;
SELECT countIf(CAST(if(number % 2 = 0, NULL, 1) AS Nullable(UInt8)), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 64;
SELECT countIf(CAST(if(number % 2 = 0, NULL, 1) AS Nullable(UInt8)), toUInt8(255)) FROM numbers(65) SETTINGS max_block_size = 65;
SELECT countIf(CAST(if(number % 2 = 0, NULL, 1) AS Nullable(UInt8)), toUInt8(128)) FROM numbers(129) SETTINGS max_block_size = 129;
