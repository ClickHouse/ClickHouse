-- Tags: no-random-settings
-- Test that non-printable characters are escaped in Vertical format (issue #85179)

SELECT 'hello\0world' AS value FORMAT Vertical;
SELECT '\x01\x02\x03' AS value FORMAT Vertical;
SELECT 'tab\there' AS value FORMAT Vertical;
SELECT 'line\nbreak' AS value FORMAT Vertical;
SELECT 'carriage\rreturn' AS value FORMAT Vertical;
SELECT 'null\0and\x01mixed\x1btext' AS value FORMAT Vertical;
SELECT 'normal text' AS value FORMAT Vertical;
