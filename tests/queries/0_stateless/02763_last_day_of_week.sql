WITH 
    toDate('2018-12-25') + number AS d, 
    toDate32(d) AS d32, 
    toDateTime(d) AS dt, 
    toDateTime64(d, 0) AS dt64
SELECT
    dt64,
    toLastDayOfWeek(d) AS wd_0,
    toLastDayOfWeek(d32) AS wd32_0,
    toLastDayOfWeek(dt) AS wdt_0,
    toLastDayOfWeek(dt64) AS wdt64_0,
    toLastDayOfWeek(d, 3) AS wd_3,
    toLastDayOfWeek(d32, 3) AS wd32_3,
    toLastDayOfWeek(dt, 3) AS wdt_3,
    toLastDayOfWeek(dt64, 3) AS wdt64_3
FROM numbers(10);

