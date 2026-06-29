SET output_format_pretty_fallback_to_vertical_min_columns = 2;
SELECT repeat('x', 100 - number) AS x, repeat('x', 100 - number) AS y FROM numbers(100) SETTINGS max_block_size = 10, output_format_pretty_fallback_to_vertical_min_table_width = 100, output_format_pretty_squash_consecutive_ms = 0 FORMAT PrettyCompact;
