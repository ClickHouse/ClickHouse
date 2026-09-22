-- `substringUTF8` slices bytes when a block holds only ASCII and code points otherwise.
-- One multi-byte character must switch it back, also for blocks larger than 64 KiB.

-- The choice is made per block, so the whole column has to arrive in one block.
SELECT min(blockSize()) AS min_block, max(blockSize()) AS max_block, sum(length(s)) + count() AS chars_bytes
FROM (SELECT if(number = 0, 'ччч' || repeat('a', 1024), repeat('a', 1030)) AS s FROM numbers(64))
SETTINGS max_block_size = 65505, max_threads = 1;

-- Multi-byte character at the start of the block.
SELECT countIf(length(substringUTF8(s, 1, 3)) = 6) AS utf8_rows, count() AS rows
FROM (SELECT if(number = 0, 'ччч' || repeat('a', 1024), repeat('a', 1030)) AS s FROM numbers(64))
SETTINGS max_block_size = 65505, max_threads = 1;

-- Multi-byte character in the last bytes of the block.
SELECT countIf(length(substringUTF8(s, -3)) = 6) AS utf8_rows, count() AS rows
FROM (SELECT if(number = 63, repeat('a', 1024) || 'ччч', repeat('a', 1030)) AS s FROM numbers(64))
SETTINGS max_block_size = 65505, max_threads = 1;
