SET output_format_pretty_row_numbers = 1;
SET output_format_pretty_glue_chunks = 1;
SET output_format_pretty_squash_consecutive_ms = 0;
SET max_block_size = 2;

SELECT sleep(0.01), number FROM numbers(11) FORMAT PrettyCompact;
SELECT sleep(0.01), number FROM numbers(11) FORMAT PrettySpace;
SELECT sleep(0.01), number FROM numbers(11) FORMAT Pretty;

SET output_format_pretty_row_numbers = 0;

SELECT sleep(0.01), number FROM numbers(11) FORMAT PrettyCompact;
SELECT sleep(0.01), number FROM numbers(11) FORMAT PrettySpace;
SELECT sleep(0.01), number FROM numbers(11) FORMAT Pretty;

SET output_format_pretty_display_footer_column_names_min_rows = 1;

SELECT sleep(0.01), number FROM numbers(11) FORMAT PrettyCompact;
SELECT sleep(0.01), number FROM numbers(11) FORMAT PrettySpace;
SELECT sleep(0.01), number FROM numbers(11) FORMAT Pretty;
