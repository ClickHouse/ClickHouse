SELECT changeYear(toDate('1970-01-01'), 2000);
SELECT changeYear(toDate32('1900-01-01'), 2001);
SELECT changeYear(toDateTime('1970-01-01 11:22:33', 'Antarctica/Palmer'), 2002);
SELECT changeYear(toDateTime64('1900-01-01 11:22:33.4444', 4, 'Antarctica/Palmer'), 2003);

SELECT changeMonth(toDate('1970-01-01'), 02);
SELECT changeMonth(toDate32('1970-01-01'), 03);
SELECT changeMonth(toDateTime('1970-01-01 11:22:33', 'Antarctica/Palmer'), 04);
SELECT changeMonth(toDateTime64('1970-01-01 11:22:33.4444', 4, 'Antarctica/Palmer'), 05);

SELECT changeDay(toDate('1970-01-01'), 02);
SELECT changeDay(toDate32('1970-01-01'), 03);
SELECT changeDay(toDateTime('1970-01-01 11:22:33', 'Antarctica/Palmer'), 04);
SELECT changeDay(toDateTime64('1970-01-01 11:22:33.4444', 4, 'Antarctica/Palmer'), 05);

SELECT toHour(changeHour(toDate('1970-01-01'), 12));
SELECT toHour(changeHour(toDate32('1970-01-01'), 13));
SELECT changeHour(toDateTime('1970-01-01 11:22:33', 'Antarctica/Palmer'), 14);
SELECT changeHour(toDateTime64('1970-01-01 11:22:33.4444', 4, 'Antarctica/Palmer'), 15);

SELECT toMinute(changeMinute(toDate('1970-01-01'), 23));
SELECT toMinute(changeMinute(toDate32('1970-01-01'), 24));
SELECT changeMinute(toDateTime('1970-01-01 11:22:33', 'Antarctica/Palmer'), 25);
SELECT changeMinute(toDateTime64('1970-01-01 11:22:33.4444', 4, 'Antarctica/Palmer'), 26);

SELECT toSecond(changeSecond(toDate('1970-01-01'), 34));
SELECT toSecond(changeSecond(toDate32('1970-01-01'), 35));
SELECT changeSecond(toDateTime('1970-01-01 11:22:33', 'Antarctica/Palmer'), 36);
SELECT changeSecond(toDateTime64('1970-01-01 11:22:33.4444', 4, 'Antarctica/Palmer'), 37);

SELECT changeYear(toDate('2000-01-01'), 1969.0);       
SELECT changeYear(toDate('2000-06-07'), 2149.0);
SELECT changeMonth(toDate('2149-01-01'), 07);
SELECT changeMonth(toDate('2000-01-01'), 13);
SELECT changeDay(toDate('2000-01-01'), 0); 
SELECT changeDay(toDate('2000-01-01'), 32);
SELECT changeHour(toDate('2000-01-01'), -1);
SELECT changeHour(toDate('2000-01-01'), 24);
SELECT changeMinute(toDate('2000-01-01'), -1);
SELECT changeMinute(toDate('2000-01-01'), 60);
SELECT changeSecond(toDate('2000-01-01'), -1);
SELECT changeSecond(toDate('2000-01-01'), 60);