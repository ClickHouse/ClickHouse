--- Despite max_block_size = 1, this will squash the blocks and output everything as a single block:
SELECT number FROM numbers(10) FORMAT PrettyCompact SETTINGS max_block_size = 1, output_format_pretty_squash_consecutive_ms = 60000, output_format_pretty_squash_max_wait_ms = 60000;
SELECT number FROM numbers(10) FORMAT PrettyCompact SETTINGS max_block_size = 1, output_format_pretty_squash_consecutive_ms = 0;
