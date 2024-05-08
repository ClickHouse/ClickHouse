SELECT changeYear(makeDate(1970, 01, 01), 2000);
SELECT changeYear(makeDate32(1970, 01, 01), 2001);
SELECT changeYear(makeDateTime(1970, 01, 01, 11, 22, 33), 2002);
SELECT changeYear(makeDateTime64(1970, 01, 01, 11, 22, 33, 4444, 4), 2003);

SELECT changeMonth(makeDate(1970, 01, 01), 02);
SELECT changeMonth(makeDate32(1970, 01, 01), 03);
SELECT changeMonth(makeDateTime(1970, 01, 01, 11, 22, 33), 04);
SELECT changeMonth(makeDateTime64(1970, 01, 01, 11, 22, 33, 4444, 4), 05);

SELECT changeDay(makeDate(1970, 01, 01), 02);
SELECT changeDay(makeDate32(1970, 01, 01), 03);
SELECT changeDay(makeDateTime(1970, 01, 01, 11, 22, 33), 04);
SELECT changeDay(makeDateTime64(1970, 01, 01, 11, 22, 33, 4444, 4), 05);

SELECT toTimeZone(changeHour(makeDate(1970, 01, 01), 12), 'UTC');
SELECT toTimeZone(changeHour(makeDate32(1970, 01, 01), 13), 'UTC');
SELECT changeHour(makeDateTime(1970, 01, 01, 11, 22, 33), 14);
SELECT changeHour(makeDateTime64(1970, 01, 01, 11, 22, 33, 4444, 4), 15);

SELECT toTimeZone(changeMinute(makeDate(1970, 01, 01), 23), 'UTC');
SELECT toTimeZone(changeMinute(makeDate32(1970, 01, 01), 24), 'UTC');
SELECT changeMinute(makeDateTime(1970, 01, 01, 11, 22, 33), 25);
SELECT changeMinute(makeDateTime64(1970, 01, 01, 11, 22, 33, 4444, 4), 26);

SELECT toTimeZone(changeSecond(makeDate(1970, 01, 01), 34), 'UTC');
SELECT toTimeZone(changeSecond(makeDate32(1970, 01, 01), 35), 'UTC');
SELECT changeSecond(makeDateTime(1970, 01, 01, 11, 22, 33), 36);
SELECT changeSecond(makeDateTime64(1970, 01, 01, 11, 22, 33, 4444, 4), 37);

SELECT changeYear(makeDate(2000, 01, 01), 1969.0);       
SELECT changeYear(makeDate(2000, 06, 07), 2149.0);
SELECT changeMonth(makeDate(2149, 01, 01), 07);
SELECT changeMonth(makeDate(2000, 06, 07), 13);
SELECT changeDay(makeDate(2000, 01, 01), 0); 
SELECT changeDay(makeDate(2000, 06, 07), 32);
SELECT toTimeZone(changeHour(makeDate(2000, 01, 01), -1), 'UTC');     
SELECT toTimeZone(changeHour(makeDate(2000, 06, 07), 24), 'UTC');
SELECT toTimeZone(changeMinute(makeDate(2000, 01, 01), -1), 'UTC'); 
SELECT toTimeZone(changeMinute(makeDate(2000, 06, 07), 60), 'UTC');
SELECT toTimeZone(changeSecond(makeDate(2000, 01, 01), -1), 'UTC');       
SELECT toTimeZone(changeSecond(makeDate(2000, 06, 07), 60), 'UTC');