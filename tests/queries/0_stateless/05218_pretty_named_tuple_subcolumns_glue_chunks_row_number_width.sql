-- A named Tuple whose name is wider than the span of its subcolumns needs a second width pass.
-- The second pass must not replace the row number width of the previous chunk with the width of
-- the current chunk: the transition from 9 to 10 rows widens the row numbers, so the chunks must
-- not be glued and the second chunk has to be redrawn with a wider left margin.

SET output_format_pretty_color = 0;
SET output_format_pretty_row_numbers = 1;
SET output_format_pretty_glue_chunks = 1;
SET output_format_pretty_squash_consecutive_ms = 0;
SET output_format_pretty_named_tuples_as_subcolumns = 1;
SET max_block_size = 9;

SELECT 'Long Tuple name, two width passes';
SELECT sleep(0.01), (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name FROM numbers(12) FORMAT PrettyCompact;
SELECT sleep(0.01), (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name FROM numbers(12) FORMAT Pretty;
SELECT sleep(0.01), (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name FROM numbers(12) FORMAT PrettySpace;

SELECT 'Short Tuple name, single width pass';
SELECT sleep(0.01), (1, 2)::Tuple(a UInt8, b UInt8) AS t FROM numbers(12) FORMAT PrettyCompact;

SELECT 'Same number of digits: the chunks are glued';
SELECT sleep(0.01), (1, 2)::Tuple(a UInt8, b UInt8) AS this_is_a_rather_long_column_name FROM numbers(15) SETTINGS max_block_size = 10 FORMAT PrettyCompact;
