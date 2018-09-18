SET send_logs_level = 'none';

SELECT formatDateTime(); -- { serverError 42 }
SELECT formatDateTime('not a datetime', 'IGNORED'); -- { serverError 43 }
SELECT formatDateTime(now(), now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'good format pattern', now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'unescaped %'); -- { serverError 36 }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%U'); -- { serverError 43 }
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%W'); -- { serverError 43 }

SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%C');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%d');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%D');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%e');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%F');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 02:33:44'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-01 00:33:44'), '%j');
SELECT formatDateTime(toDateTime('2000-12-31 00:33:44'), '%j');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%m');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%M');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%n');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 12:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%R');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%S');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%t');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%T');
SELECT formatDateTime(toDateTime('2018-01-01 22:33:44'), '%u'), formatDateTime(toDateTime('2018-01-07 22:33:44'), '%u');
SELECT formatDateTime(toDateTime('1996-01-01 22:33:44'), '%V'), formatDateTime(toDateTime('1996-12-31 22:33:44'), '%V'),
       formatDateTime(toDateTime('1999-01-01 22:33:44'), '%V'), formatDateTime(toDateTime('1999-12-31 22:33:44'), '%V');
SELECT formatDateTime(toDateTime('2018-01-01 22:33:44'), '%w'), formatDateTime(toDateTime('2018-01-07 22:33:44'), '%w');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%y');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%Y');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%%');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), 'no formatting pattern');

SELECT formatDateTime(toDate('2018-01-01'), '%F %T');
SELECT
    formatDateTime(toDateTime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'UTC'),
    formatDateTime(toDateTime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'Europe/Moscow')