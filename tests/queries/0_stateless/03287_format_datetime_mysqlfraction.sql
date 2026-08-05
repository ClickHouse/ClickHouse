-- Test for issue #72879

SELECT 'Default settings';
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.12345678', 8, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 6, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.1234', 4, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.12', 2, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.1', 1, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00', 0, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 0;

SELECT 'Compatibility settings';
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.12345678', 8, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 6, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.1234', 4, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.12', 2, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00.1', 1, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
SELECT formatDateTime(toDateTime64('1970-01-01 00:00:00', 0, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
