SET output_format_pretty_color = 1, output_format_pretty_highlight_digit_groups = 1, output_format_pretty_single_large_number_tip_threshold = 1;
SELECT exp2(number), exp10(number), 'test'||number FROM numbers(64) FORMAT Vertical;

SET output_format_pretty_color = 0, output_format_pretty_highlight_digit_groups = 1, output_format_pretty_single_large_number_tip_threshold = 1;
SELECT exp2(number), exp10(number), 'test'||number FROM numbers(64) FORMAT Vertical;

SET output_format_pretty_color = 1, output_format_pretty_highlight_digit_groups = 0, output_format_pretty_single_large_number_tip_threshold = 1;
SELECT exp2(number), exp10(number), 'test'||number FROM numbers(64) FORMAT Vertical;

SET output_format_pretty_color = 0, output_format_pretty_highlight_digit_groups = 0, output_format_pretty_single_large_number_tip_threshold = 0;
SELECT exp2(number), exp10(number), 'test'||number FROM numbers(64) FORMAT Vertical;
