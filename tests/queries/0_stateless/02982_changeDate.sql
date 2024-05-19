SELECT changeYear(toDate('2000-01-01'), 2001);
SELECT changeYear(toDate32('2000-01-01'), 2002);
SELECT changeYear(toDateTime('2000-01-01 11:22:33'), 2003);
SELECT changeYear(toDateTime64('2000-01-01 11:22:33.4444', 4), 2004);

SELECT changeMonth(toDate('2000-01-01'), 02);
SELECT changeMonth(toDate32('2000-01-01'), 03);
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), 04);
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), 05);

SELECT changeDay(toDate('2000-01-01'), 02);
SELECT changeDay(toDate32('2000-01-01'), 03);
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), 04);
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), 05);

SELECT changeHour(toDate('2000-01-01'), 12);
SELECT changeHour(toDate32('2000-01-01'), 13);
SELECT changeHour(toDateTime('2000-01-01 11:22:33'), 14);
SELECT changeHour(toDateTime64('2000-01-01 11:22:33.4444', 4), 15);

SELECT changeMinute(toDate('2000-01-01'), 23);
SELECT changeMinute(toDate32('2000-01-01'), 24);
SELECT changeMinute(toDateTime('2000-01-01 11:22:33'), 25);
SELECT changeMinute(toDateTime64('2000-01-01 11:22:33.4444', 4), 26);

SELECT changeSecond(toDate('2000-01-01'), 34);
SELECT changeSecond(toDate32('2000-01-01'), 35);
SELECT changeSecond(toDateTime('2000-01-01 11:22:33'), 36);
SELECT changeSecond(toDateTime64('2000-01-01 11:22:33.4444', 4), 37);

SELECT changeYear(toDate('2000-01-01'), 1969.0);
SELECT changeYear(toDate('2000-06-07'), 2149.0);
SELECT changeMonth(toDate('2149-01-01'), 07);
SELECT changeMonth(toDate('2000-01-01'), 13);
SELECT changeDay(toDate('2000-01-01'), 0); 
SELECT changeDay(toDate('2000-01-01'), 32);
SELECT changeHour(toDate('2000-01-01'), -1) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeHour(toDate('2000-01-01'), 24) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeMinute(toDate('2000-01-01'), -1) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeMinute(toDate('2000-01-01'), 60) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeSecond(toDate('2000-01-01'), -1) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeSecond(toDate('2000-01-01'), 60) SETTINGS session_timezone = 'Asia/Novosibirsk';
