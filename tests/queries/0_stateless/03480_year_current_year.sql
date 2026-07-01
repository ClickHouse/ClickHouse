-- Zero-argument year() returns the current year, mirroring now()/today().
-- Implemented as a parser rewrite year() -> toYear(today()), so year(<date>) keeps
-- behaving as the deterministic alias of toYear.

SELECT '-- year() equals toYear(today()), toYear(now()) and year(today())';
SELECT year() = toYear(today()) AS eq_today,
       year() = toYear(now()) AS eq_now,
       year() = year(today()) AS eq_year_today;

SELECT '-- result type is UInt16';
SELECT toTypeName(year());

SELECT '-- current year is a sane value';
SELECT year() BETWEEN 2020 AND 2100;

SELECT '-- case-insensitive';
SELECT YEAR() = year() AS a, Year() = year() AS b, yEaR() = year() AS c;

SELECT '-- year() is non-deterministic (evaluated once at analysis, like today())';
SELECT isConstant(year());

SELECT '-- year(<date>) still works as the deterministic alias of toYear';
SELECT year(toDate('2023-04-21')),
       year(toDate32('1900-01-01')),
       year(toDateTime('2077-12-31 23:59:59')),
       YEAR(toDateTime64('2149-06-06 00:00:00', 3));

SELECT '-- two-argument toYear(datetime, timezone) is unaffected';
SELECT toYear(toDateTime('2023-01-01 00:00:00', 'UTC'), 'Asia/Yekaterinburg');

SELECT '-- the rewrite is visible in EXPLAIN SYNTAX';
EXPLAIN SYNTAX SELECT year();
