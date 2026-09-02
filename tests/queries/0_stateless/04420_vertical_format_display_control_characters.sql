-- Non-printable control characters are displayed as Unicode "Control Pictures" in Vertical format (issue #85179).

SELECT 'hello\0world' AS value FORMAT Vertical;
SELECT '\x01\x02\x03' AS value FORMAT Vertical;
SELECT 'tab\there' AS value FORMAT Vertical;
SELECT 'line\nbreak' AS value FORMAT Vertical;
SELECT 'carriage\rreturn' AS value FORMAT Vertical;
SELECT 'null\0and\x01mixed\x1btext' AS value FORMAT Vertical;
SELECT 'delete\x7Fchar' AS value FORMAT Vertical;
SELECT 'normal text' AS value FORMAT Vertical;

-- A literal backslash sequence in the data stays intact and is not confused with a control character.
SELECT 'literal\\0backslash' AS value FORMAT Vertical;

-- Multi-byte UTF-8 characters are preserved.
SELECT 'snowman ☃ and é' AS value FORMAT Vertical;

-- The setting can be disabled to print raw bytes (the old behavior).
SELECT 'tab\tand\nnewline' AS value FORMAT Vertical SETTINGS output_format_vertical_display_control_characters = 0;
