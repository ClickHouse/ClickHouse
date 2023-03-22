SET send_logs_level = 'fatal';

SELECT formatDateTime(); -- { serverError 42 }
SELECT formatDateTime('not a datetime', 'IGNORED'); -- { serverError 43 }
SELECT formatDateTime(now(), now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'good format pattern', now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'unescaped %'); -- { serverError 36 }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%U'); -- { serverError 48 }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%W'); -- { serverError 48 }

SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%C'), formatDateTime(toDate32('2018-01-02'), '%C');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%d'), formatDateTime(toDate32('2018-01-02'), '%d');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%D'), formatDateTime(toDate32('2018-01-02'), '%D');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%e'), formatDateTime(toDate32('2018-01-02'), '%e');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%F'), formatDateTime(toDate32('2018-01-02'), '%F');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%H'), formatDateTime(toDate32('2018-01-02'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 02:33:44'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%I'), formatDateTime(toDate32('2018-01-02'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-01 00:33:44'), '%j'), formatDateTime(toDate32('2018-01-01'), '%j');
SELECT formatDateTime(toDateTime('2000-12-31 00:33:44'), '%j'), formatDateTime(toDate32('2000-12-31'), '%j');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%m'), formatDateTime(toDate32('2018-01-02'), '%m');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%M'), formatDateTime(toDate32('2018-01-02'), '%M');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%n'), formatDateTime(toDate32('2018-01-02'), '%n');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%p'), formatDateTime(toDateTime('2018-01-02'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 12:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%R'), formatDateTime(toDate32('2018-01-02'), '%R');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%S'), formatDateTime(toDate32('2018-01-02'), '%S');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%t'), formatDateTime(toDate32('2018-01-02'), '%t');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%T'), formatDateTime(toDate32('2018-01-02'), '%T');
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
