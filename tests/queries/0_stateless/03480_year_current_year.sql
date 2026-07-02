-- Zero-argument year() returns the current year, mirroring now()/today().
-- Implemented as a proper SQL function (src/Functions/year.cpp): with arguments it
-- delegates to toYear, without arguments it returns the current year (non-deterministic,
-- like today()).

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

SELECT '-- year() is a constant expression (evaluated once at analysis, like today())';
SELECT isConstant(year());

SELECT '-- year(<date>) delegates to toYear across date/time types';
SELECT year(toDate('2023-04-21')),
       year(toDate32('1900-01-01')),
       year(toDateTime('2077-12-31 23:59:59')),
       YEAR(toDateTime64('2149-06-06 00:00:00', 3));

SELECT '-- year(datetime, timezone) matches toYear(datetime, timezone)';
SELECT year(toDateTime('2023-01-01 00:00:00', 'UTC'), 'Asia/Yekaterinburg') = toYear(toDateTime('2023-01-01 00:00:00', 'UTC'), 'Asia/Yekaterinburg');

SELECT '-- system.functions exposes year with its own metadata (own row, not an alias)';
SELECT name, deterministic, alias_to FROM system.functions WHERE name = 'year';

SELECT '-- year(<key>) keeps toYear index/monotonicity analysis (same granule pruning)';
DROP TABLE IF EXISTS 03480_year_tbl;
CREATE TABLE 03480_year_tbl (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 8192;
INSERT INTO 03480_year_tbl SELECT toDate('2000-01-01') + number FROM numbers(40000);
SELECT (SELECT count() FROM 03480_year_tbl WHERE year(d) = 2005) = (SELECT count() FROM 03480_year_tbl WHERE toYear(d) = 2005) AS same_result;
SELECT
    (SELECT rows FROM (EXPLAIN ESTIMATE SELECT count() FROM 03480_year_tbl WHERE year(d) = 2005))
  = (SELECT rows FROM (EXPLAIN ESTIMATE SELECT count() FROM 03480_year_tbl WHERE toYear(d) = 2005)) AS same_index_scan;
DROP TABLE 03480_year_tbl;
