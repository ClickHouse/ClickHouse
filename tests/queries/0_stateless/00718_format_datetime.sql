SET send_logs_level = 'fatal';

SELECT formatDateTime(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatDateTime('not a datetime', 'IGNORED'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatDateTime(now(), now()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatDateTime(now(), 'good format pattern', now()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatDateTime(now(), 'unescaped %'); -- { serverError BAD_ARGUMENTS }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%U'); -- { serverError NOT_IMPLEMENTED }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%v'); -- { serverError NOT_IMPLEMENTED }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%x'); -- { serverError NOT_IMPLEMENTED }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%X'); -- { serverError NOT_IMPLEMENTED }

SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%a'), formatDateTime(toDate32('2018-01-02'), '%a');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%b'), formatDateTime(toDate32('2018-01-02'), '%b');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%c'), formatDateTime(toDate32('2018-01-02'), '%c');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%C'), formatDateTime(toDate32('2018-01-02'), '%C');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%d'), formatDateTime(toDate32('2018-01-02'), '%d');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%D'), formatDateTime(toDate32('2018-01-02'), '%D');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%e'), formatDateTime(toDate32('2018-01-02'), '%e');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%f'), formatDateTime(toDate32('2018-01-02'), '%f');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%F'), formatDateTime(toDate32('2018-01-02'), '%F');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%h'), formatDateTime(toDate32('2018-01-02'), '%h');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%H'), formatDateTime(toDate32('2018-01-02'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 02:33:44'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%i'), formatDateTime(toDate32('2018-01-02'), '%i');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%I'), formatDateTime(toDate32('2018-01-02'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-01 00:33:44'), '%j'), formatDateTime(toDate32('2018-01-01'), '%j');
SELECT formatDateTime(toDateTime('2000-12-31 00:33:44'), '%j'), formatDateTime(toDate32('2000-12-31'), '%j');
SELECT formatDateTime(toDateTime('2000-12-31 00:33:44'), '%k'), formatDateTime(toDate32('2000-12-31'), '%k');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%m'), formatDateTime(toDate32('2018-01-02'), '%m');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%M'), formatDateTime(toDate32('2018-01-02'), '%M') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 1;
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%M'), formatDateTime(toDate32('2018-01-02'), '%M') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%n'), formatDateTime(toDate32('2018-01-02'), '%n');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%p'), formatDateTime(toDateTime('2018-01-02'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 12:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%r'), formatDateTime(toDate32('2018-01-02'), '%r');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%R'), formatDateTime(toDate32('2018-01-02'), '%R');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%S'), formatDateTime(toDate32('2018-01-02'), '%S');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%t'), formatDateTime(toDate32('2018-01-02'), '%t');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%T'), formatDateTime(toDate32('2018-01-02'), '%T');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%W'), formatDateTime(toDate32('2018-01-02'), '%W');
SELECT formatDateTime(toDateTime('2018-01-01 22:33:44'), '%u'), formatDateTime(toDateTime('2018-01-07 22:33:44'), '%u'),
       formatDateTime(toDate32('2018-01-01'), '%u'), formatDateTime(toDate32('2018-01-07'), '%u');
SELECT formatDateTime(toDateTime('1996-01-01 22:33:44'), '%V'), formatDateTime(toDateTime('1996-12-31 22:33:44'), '%V'),
       formatDateTime(toDateTime('1999-01-01 22:33:44'), '%V'), formatDateTime(toDateTime('1999-12-31 22:33:44'), '%V'),
       formatDateTime(toDate32('1996-01-01'), '%V'), formatDateTime(toDate32('1996-12-31'), '%V'),
       formatDateTime(toDate32('1999-01-01'), '%V'), formatDateTime(toDate32('1999-12-31'), '%V');
SELECT formatDateTime(toDateTime('2018-01-01 22:33:44'), '%w'), formatDateTime(toDateTime('2018-01-07 22:33:44'), '%w'),
       formatDateTime(toDate32('2018-01-01'), '%w'), formatDateTime(toDate32('2018-01-07'), '%w');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%y'), formatDateTime(toDate32('2018-01-02'), '%y');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%Y'), formatDateTime(toDate32('2018-01-02'), '%Y');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%%'), formatDateTime(toDate32('2018-01-02'), '%%');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), 'no formatting pattern'), formatDateTime(toDate32('2018-01-02'), 'no formatting pattern');

SELECT formatDateTime(toDate('2018-01-01'), '%F %T');
SELECT formatDateTime(toDate32('1927-01-01'), '%F %T');

SELECT
    formatDateTime(toDateTime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'UTC'),
    formatDateTime(toDateTime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'Asia/Istanbul');

SELECT formatDateTime(toDateTime('2020-01-01 01:00:00', 'UTC'), '%z');
SELECT formatDateTime(toDateTime('2020-01-01 01:00:00', 'US/Samoa'), '%z');
SELECT formatDateTime(toDateTime('2020-01-01 01:00:00', 'Europe/Moscow'), '%z');
SELECT formatDateTime(toDateTime('1970-01-01 00:00:00', 'Asia/Kolkata'), '%z');

-- %f (default settings)
select formatDateTime(toDate('2010-01-04'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDate32('2010-01-04'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime('2010-01-04 12:34:56'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime64('2010-01-04 12:34:56', 0), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123', 3), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 6), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456789', 9), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
-- %f (legacy settings)
select formatDateTime(toDate('2010-01-04'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
select formatDateTime(toDate32('2010-01-04'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
select formatDateTime(toDateTime('2010-01-04 12:34:56'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
select formatDateTime(toDateTime64('2010-01-04 12:34:56', 0), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123', 3), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 6), '%f') SETTINGS formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456789', 9), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;

select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 9, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 1, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 0, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
select formatDateTime(toDate32('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
select formatDateTime(toDate('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');

-- %c %k %l with different formatdatetime_format_without_leading_zeros
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%c') settings formatdatetime_format_without_leading_zeros = 0;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%m') settings formatdatetime_format_without_leading_zeros = 0;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%k') settings formatdatetime_format_without_leading_zeros = 0;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%l') settings formatdatetime_format_without_leading_zeros = 0;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%h') settings formatdatetime_format_without_leading_zeros = 0;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%c') settings formatdatetime_format_without_leading_zeros = 1;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%m') settings formatdatetime_format_without_leading_zeros = 1;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%k') settings formatdatetime_format_without_leading_zeros = 1;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%l') settings formatdatetime_format_without_leading_zeros = 1;
select formatDateTime(toDateTime('2022-01-08 02:11:29', 'UTC'), '%h') settings formatdatetime_format_without_leading_zeros = 1;
