-- By default, the week contains January 1 is the first week:
SELECT toDate('2016-12-22') + number AS d, toCustomWeek(d) as week, toCustomYear(d) AS year, toStartOfCustomYear(d) AS startday FROM numbers(15);
-- The week contains January 28 is the first week:
SELECT toDateTime(toDate('2017-01-20') + number, 'Europe/Moscow' ) AS d, toCustomWeek(d,'01-28') as week, toCustomYear(d,'01-28') AS year, toStartOfCustomYear(d,'01-28') AS startday FROM numbers(15);
-- and with timezone
SELECT toDateTime(toDate('2017-01-20') + number, 'Europe/Moscow' ) AS d, toCustomWeek(d,'01-28','Europe/Moscow') as week, toCustomYear(d,'01-28','Europe/Moscow') AS year, toStartOfCustomYear(d,'01-28','Europe/Moscow') AS startday FROM numbers(15);
