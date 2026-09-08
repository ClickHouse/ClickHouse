-- Keep printing raw control characters (the license text ends with a newline) so the reference is stable.
SET output_format_vertical_display_control_characters = 0;
SELECT * REPLACE substring(license_text, 1, position(license_text, '\n')) AS license_text FROM system.licenses WHERE library_name = 'poco' FORMAT Vertical;
