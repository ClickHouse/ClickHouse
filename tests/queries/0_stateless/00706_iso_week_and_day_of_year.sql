-- International Programmers' Day
SELECT toDayOfYear(toDate('2018-09-13'));

SELECT toDate('2018-09-17') AS x, toDateTime(x) AS x_t, toISOWeek(x), toISOWeek(x_t), toISOYear(x), toISOYear(x_t), toStartOfISOYear(x), toStartOfISOYear(x_t);

SELECT toDate('2018-12-25') + number AS x, toDateTime(x) AS x_t, toISOWeek(x) AS w, toISOWeek(x_t) AS wt, toISOYear(x) AS y, toISOYear(x_t) AS yt, toStartOfISOYear(x) AS ys, toStartOfISOYear(x_t) AS yst, toDayOfYear(x) AS dy, toDayOfYear(x_t) AS dyt FROM system.numbers LIMIT 10;
SELECT toDate('2016-12-25') + number AS x, toDateTime(x) AS x_t, toISOWeek(x) AS w, toISOWeek(x_t) AS wt, toISOYear(x) AS y, toISOYear(x_t) AS yt, toStartOfISOYear(x) AS ys, toStartOfISOYear(x_t) AS yst, toDayOfYear(x) AS dy, toDayOfYear(x_t) AS dyt FROM system.numbers LIMIT 10;

-- ISO year always begins at monday.
SELECT DISTINCT toDayOfWeek(toStartOfISOYear(toDateTime(1000000000 + rand64() % 1000000000))) FROM numbers(10000);
SELECT DISTINCT toDayOfWeek(toStartOfISOYear(toDate(10000 + rand64() % 20000))) FROM numbers(10000);

-- Year and ISO year don't differ by more than one.
WITH toDateTime(1000000000 + rand64() % 1000000000) AS time SELECT max(abs(toYear(time) - toISOYear(time))) <= 1 FROM numbers(10000);

-- ISO week is between 1 and 53
WITH toDateTime(1000000000 + rand64() % 1000000000) AS time SELECT DISTINCT toISOWeek(time) BETWEEN 1 AND 53 FROM numbers(1000000);
