-- Blocks that arrive after `output_format_pretty_max_rows` is reached are not rendered, but are counted in the footer.
SET output_format_pretty_squash_consecutive_ms = 0;
SET output_format_pretty_max_rows = 4;

SELECT number FROM system.numbers LIMIT 10 SETTINGS max_block_size = 2 FORMAT PrettyCompact;
SELECT number FROM system.numbers LIMIT 10 SETTINGS max_block_size = 3 FORMAT PrettyCompact;
