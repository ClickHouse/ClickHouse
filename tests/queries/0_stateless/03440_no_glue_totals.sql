SET output_format_pretty_row_numbers = 1;
SET output_format_pretty_glue_chunks = 1;
SET output_format_pretty_squash_consecutive_ms = 0;
SET max_threads = 1;
SET max_block_size = 1;

SELECT number, count() FROM numbers(5) GROUP BY number WITH TOTALS ORDER BY number FORMAT PrettyCompact;
