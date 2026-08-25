-- countIfOrNull with a condition that is a UInt8 value other than 0 or 1 must return the same
-- count whatever the block size is. max_block_size pins the block length.

SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 63;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 64;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(2)) FROM numbers(100) SETTINGS max_block_size = 100;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(255)) FROM numbers(63) SETTINGS max_block_size = 63;
SELECT countIfOrNull(if(number % 2 = 0, NULL, 1), toUInt8(128)) FROM numbers(129) SETTINGS max_block_size = 129;
