SET output_format_pretty_display_footer_column_names=0;
SET output_format_pretty_max_column_pad_width = 250;
SELECT range(number) FROM system.numbers LIMIT 100 FORMAT PrettyCompactNoEscapes;
