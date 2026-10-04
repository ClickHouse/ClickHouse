-- Non-printable control characters are displayed as Unicode "Control Pictures" in the `Pretty*`
-- formats too, not only in `Vertical` (issue #85179).

SET output_format_pretty_display_footer_column_names = 0;
SET output_format_pretty_color = 0;
-- Test the `Pretty*` formats themselves, not the fallback to `Vertical`.
SET output_format_pretty_fallback_to_vertical = 0;

-- The table outline is preserved, because every Control Picture takes exactly one visible position,
-- while the raw control character takes none.
SELECT 'hello\0world' AS value FORMAT PrettyCompact;
-- A tab is never replaced: a terminal advances to the next tab stop, and `computeWidth` measures
-- it that way, so the value is left alone.
SELECT 'tab\there' AS value FORMAT PrettyCompact;
SELECT 'delete\x7Fchar' AS value FORMAT PrettyCompact;
SELECT '\x01\x02\x03' AS value FORMAT PrettyCompact;

-- The other `Pretty` styles behave the same way.
SELECT 'nul\0here' AS value FORMAT Pretty;
SELECT 'nul\0here' AS value FORMAT PrettySpace;

-- Control characters in column names are displayed as well, and their Control Pictures are taken
-- into account when the columns are padded to the same width.
SELECT 1 AS `sohere\x01` FORMAT PrettyCompact;
SELECT 1 AS `nul\0name`, 2 AS ok FORMAT PrettyCompact;
-- A line feed in a column name is replaced as well, unlike one in a value: the header and the
-- footer are a single line, so it would only deform the table.
SELECT 1 AS `line\nbreak`, 2 AS ok FORMAT PrettyCompact;

-- A line feed is never replaced: it becomes a new line of the table cell...
SELECT 'line\nbreak' AS value FORMAT PrettyCompact;
-- ...or, with multi-line fields off, is emitted as is so that the value stays easy to copy-paste.
SELECT 'line\nbreak' AS value FORMAT PrettyCompact SETTINGS output_format_pretty_multiline_fields = 0;

-- `ESC` is never replaced either, so that the ANSI escape sequences carried by the data keep being
-- interpreted by the terminal. They take no visible position, so the table outline is preserved.
SELECT '\x1b[31mred\x1b[0m' AS value FORMAT PrettyCompact;

-- Trailing whitespace is still highlighted, including a carriage return, whose picture form gets
-- the highlighting: it is detected on the bytes before the replacement, in one pass.
SELECT 'spaces  ' AS a, 'tab\t' AS b, 'cr\r' AS c, 'mixed \t\r' AS d FORMAT PrettyCompact
SETTINGS output_format_pretty_color = 1, output_format_pretty_highlight_trailing_spaces = 1;

-- The trailing whitespace of each line of a multi-line value is highlighted on its own.
SELECT 'a\t\nbb  \nccc' AS v FORMAT PrettyCompact
SETTINGS output_format_pretty_color = 1, output_format_pretty_highlight_trailing_spaces = 1;

-- Multi-byte UTF-8 characters are preserved.
SELECT 'snowman ☃ and é' AS value FORMAT PrettyCompact;

-- The setting can be disabled to print raw bytes (the old behavior).
SELECT 'nul\0here' AS value FORMAT PrettyCompact SETTINGS output_format_pretty_display_control_characters = 0;
