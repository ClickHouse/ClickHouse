SELECT number, number + 1 FROM numbers(2) SETTINGS max_block_size = 1 FORMAT Hash;
SELECT number, number + 1 FROM numbers(2) SETTINGS max_block_size = 2 FORMAT Hash;
