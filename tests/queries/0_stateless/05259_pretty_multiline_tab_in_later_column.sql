-- A tab advances to the next tab stop, so the width of a line of a multi-line value depends on the
-- visible position where its column starts. It has to be measured from the same position when the
-- table is printed as when the column widths are calculated, otherwise the row gets wider than its border.

SET output_format_pretty_display_footer_column_names = 0;
SET output_format_pretty_color = 0;
SET output_format_pretty_fallback_to_vertical = 0;
SET output_format_pretty_multiline_fields = 1;

SELECT 'x' AS a, 'tab\there\nnext' AS b FORMAT PrettyCompact SETTINGS output_format_pretty_row_numbers = 0;
SELECT 'x' AS a, 'tab\there\nnext' AS b FORMAT PrettyCompact SETTINGS output_format_pretty_row_numbers = 1;
SELECT 'x' AS a, 'tab\there\nnext' AS b FORMAT Pretty SETTINGS output_format_pretty_row_numbers = 0;
SELECT 'x' AS a, 'tab\there\nnext' AS b FORMAT PrettySpace SETTINGS output_format_pretty_row_numbers = 0;
SELECT 'longer' AS a, 'y' AS b, 'a\tb\nlonger\tline\tc' AS c FORMAT PrettyCompact SETTINGS output_format_pretty_row_numbers = 0;
