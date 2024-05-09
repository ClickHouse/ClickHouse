SELECT changeYear(toDate('1970-01-01', 'UTC'), 2000);
SELECT changeYear(toDate32('1900-01-01', 'UTC'), 2001);
SELECT changeYear(toDateTime('1970-01-01 11:22:33', 'UTC'), 2002);
SELECT changeYear(toDateTime64('1900-01-01 11:22:33.4444', 4, 'UTC'), 2003);

SELECT changeMonth(toDate('1970-01-01', 'UTC'), 02);
SELECT changeMonth(toDate32('1970-01-01', 'UTC'), 03);
SELECT changeMonth(toDateTime('1970-01-01 11:22:33', 'UTC'), 04);
SELECT changeMonth(toDateTime64('1970-01-01 11:22:33.4444', 4, 'UTC'), 05);

SELECT changeDay(toDate('1970-01-01', 'UTC'), 02);
SELECT changeDay(toDate32('1970-01-01', 'UTC'), 03);
SELECT changeDay(toDateTime('1970-01-01 11:22:33', 'UTC'), 04);
SELECT changeDay(toDateTime64('1970-01-01 11:22:33.4444', 4, 'UTC'), 05);

SELECT toTimeZone(changeHour(toDate('1970-01-01', 'UTC'), 12), 'UTC');
SELECT toTimeZone(changeHour(toDate32('1970-01-01', 'UTC'), 13), 'UTC');
SELECT changeHour(toDateTime('1970-01-01 11:22:33', 'UTC'), 14);
SELECT changeHour(toDateTime64('1970-01-01 11:22:33.4444', 4, 'UTC'), 15);

SELECT toTimeZone(changeMinute(toDate('1970-01-01', 'UTC'), 23), 'UTC');
SELECT toTimeZone(changeMinute(toDate32('1970-01-01', 'UTC'), 24), 'UTC');
SELECT changeMinute(toDateTime('1970-01-01 11:22:33', 'UTC'), 25);
SELECT changeMinute(toDateTime64('1970-01-01 11:22:33.4444', 4, 'UTC'), 26);

SELECT toTimeZone(changeSecond(toDate('1970-01-01', 'UTC'), 34), 'UTC');
SELECT toTimeZone(changeSecond(toDate32('1970-01-01', 'UTC'), 35), 'UTC');
SELECT changeSecond(toDateTime('1970-01-01 11:22:33', 'UTC'), 36);
SELECT changeSecond(toDateTime64('1970-01-01 11:22:33.4444', 4, 'UTC'), 37);

SELECT changeYear(toDate('2000-01-01', 'UTC'), 1969.0);       
SELECT changeYear(toDate('2000-06-07', 'UTC'), 2149.0);
SELECT changeMonth(toDate('2149-01-01', 'UTC'), 07);
SELECT changeMonth(toDate('2000-01-01', 'UTC'), 13);
SELECT changeDay(toDate('2000-01-01', 'UTC'), 0); 
SELECT changeDay(toDate('2000-01-01', 'UTC'), 32);
SELECT toTimeZone(changeHour(toDate('2000-01-01', 'UTC'), -1), 'UTC');     
SELECT toTimeZone(changeHour(toDate('2000-01-01', 'UTC'), 24), 'UTC');
SELECT toTimeZone(changeMinute(toDate('2000-01-01', 'UTC'), -1), 'UTC'); 
SELECT toTimeZone(changeMinute(toDate('2000-01-01', 'UTC'), 60), 'UTC');
SELECT toTimeZone(changeSecond(toDate('2000-01-01', 'UTC'), -1), 'UTC');       
SELECT toTimeZone(changeSecond(toDate('2000-01-01', 'UTC'), 60), 'UTC');