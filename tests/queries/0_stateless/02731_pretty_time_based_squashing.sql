SELECT * FROM numbers(3) FORMAT PrettyCompact SETTINGS max_block_size = 1, max_threads = 1, output_format_pretty_squash_ms = 0;
SELECT * FROM numbers(3) FORMAT PrettyCompact SETTINGS max_block_size = 1, max_threads = 1, output_format_pretty_squash_ms = 600_000;
