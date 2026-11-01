-- Non-printable control characters are displayed as Unicode "Control Pictures" in Vertical format (issue #85179).

SELECT 'hello\0world' AS value FORMAT Vertical;
SELECT '\x01\x02\x03' AS value FORMAT Vertical;
SELECT 'tab\there' AS value FORMAT Vertical;
SELECT 'line\nbreak' AS value FORMAT Vertical;
SELECT 'carriage\rreturn' AS value FORMAT Vertical;
SELECT 'null\0and\x01mixed\x1btext' AS value FORMAT Vertical;
SELECT 'delete\x7Fchar' AS value FORMAT Vertical;
SELECT 'normal text' AS value FORMAT Vertical;

-- `TAB`, the line feed and `ESC` are never replaced, because a terminal interprets them rather
-- than swallowing them: the `tab\there` value above keeps its tab, the `line\nbreak` value stays
-- on two lines, and the ANSI escape sequences carried by the data keep being interpreted.
SELECT '\x1b[31mred\x1b[0m' AS value FORMAT Vertical;
SELECT 1 AS `esc\x1bname` FORMAT Vertical;

-- A literal backslash sequence in the data stays intact and is not confused with a control character.
SELECT 'literal\\0backslash' AS value FORMAT Vertical;

-- Multi-byte UTF-8 characters are preserved.
SELECT 'snowman ☃ and é' AS value FORMAT Vertical;

-- The setting can be disabled to print raw bytes (the old behavior).
SELECT 'tab\tand\nnewline' AS value FORMAT Vertical SETTINGS output_format_pretty_display_control_characters = 0;

-- Control characters in column names are displayed as well, and their Control Pictures are taken into
-- account when the names are padded to the same width.
SELECT 1 AS `tab\there` FORMAT Vertical;
-- A line feed in a column name is replaced as well, unlike one in a value: a name is padded to a
-- fixed width on a single line, so it would only deform the output.
SELECT 1 AS `line\nbreak`, 2 AS ok FORMAT Vertical;
SELECT 1 AS `tab\there` FORMAT Vertical SETTINGS output_format_pretty_display_control_characters = 0;
