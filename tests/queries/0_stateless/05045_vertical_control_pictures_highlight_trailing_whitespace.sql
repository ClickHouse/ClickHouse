-- Trailing-whitespace highlighting must survive the default-on conversion of control characters
-- to Unicode Control Pictures in the Vertical format: the trailing whitespace is detected on the
-- original bytes, and its picture form (e.g. tab as a picture) is highlighted.
SET output_format_pretty_color = 1;
SET output_format_pretty_highlight_trailing_spaces = 1;
SET output_format_vertical_display_control_characters = 1;

SELECT
    'plain' AS no_whitespace,
    'spaces  ' AS trailing_spaces,
    'tab\t' AS trailing_tab,
    'newline\n' AS trailing_newline,
    'mixed  \t\n' AS trailing_mixed,
    'inner\ttab ' AS inner_control_trailing_space,
    '  ' AS only_whitespace,
    '' AS empty
FORMAT Vertical;

-- The same values with highlighting disabled: pictures are shown without ANSI escape sequences.
SET output_format_pretty_highlight_trailing_spaces = 0;

SELECT
    'spaces  ' AS trailing_spaces,
    'mixed  \t\n' AS trailing_mixed
FORMAT Vertical;
