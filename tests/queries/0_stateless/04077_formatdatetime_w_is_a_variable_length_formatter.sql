-- Formatter %W in function 'formatDateTime' is a variable-length formatter
-- In Bug 101844, this was the case only for some combinations of extra formatting settings

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (d Date) ENGINE = MergeTree ORDER BY d;

INSERT INTO tab SELECT toDate('2026-04-06') + number FROM numbers(7);

SELECT '--- Test with formatdatetime_parsedatetime_m_is_month_name:';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_parsedatetime_m_is_month_name = 1;
SELECT '---';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;

SELECT '--- Test with formatdatetime_f_prints_single_zero:';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_f_prints_single_zero = 0;
SELECT '---';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_f_prints_single_zero = 1;

SELECT '--- Test with formatdatetime_f_prints_scale_number_of_digits:';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;
SELECT '---';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;

SELECT '--- Test with formatdatetime_format_without_leading_zeros:';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_format_without_leading_zeros = 0;
SELECT '---';
SELECT formatDateTime(d, '%W %d') FROM tab ORDER BY d SETTINGS formatdatetime_format_without_leading_zeros = 1;

DROP TABLE tab;
