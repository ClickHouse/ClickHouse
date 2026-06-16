SET output_format_pretty_color=1;
SET output_format_pretty_display_footer_column_names=0;
SET output_format_pretty_squash_consecutive_ms = 0;
SELECT 1 FORMAT PrettySpace;
SELECT 1 UNION ALL SELECT 1 FORMAT PrettySpace;
SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 FORMAT PrettySpace;
