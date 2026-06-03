SET output_format_pretty_display_footer_column_names=0;
SET output_format_pretty_row_numbers = 0;

SELECT exp10(number) * (number % 2 ? 1 : -1) FROM numbers(30) FORMAT PrettySpace SETTINGS output_format_pretty_color = 1;

SELECT exp10(number) FROM numbers(10) FORMAT PrettySpace SETTINGS output_format_pretty_color = 1, output_format_pretty_highlight_digit_groups = 0;
SELECT exp10(number) FROM numbers(10) FORMAT PrettySpace;

SELECT exp10(number) + exp10(-number) FROM numbers(10) FORMAT PrettySpace SETTINGS output_format_pretty_color = 1;
