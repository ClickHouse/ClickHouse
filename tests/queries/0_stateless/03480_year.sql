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

SELECT '-- year is non-deterministic and is not an alias of toYear';
SELECT name, deterministic, alias_to FROM system.functions WHERE name = 'year';

SELECT '-- year(<key>) keeps toYear index/monotonicity analysis (same granule pruning)';
DROP TABLE IF EXISTS 03480_year_tbl;
CREATE TABLE 03480_year_tbl (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 8192;
INSERT INTO 03480_year_tbl SELECT toDate('2000-01-01') + number FROM numbers(40000);
SELECT (SELECT count() FROM 03480_year_tbl WHERE year(d) = 2005) = (SELECT count() FROM 03480_year_tbl WHERE toYear(d) = 2005) AS same_result;
SELECT
    (SELECT rows FROM (EXPLAIN ESTIMATE SELECT count() FROM 03480_year_tbl WHERE year(d) = 2005))
  = (SELECT rows FROM (EXPLAIN ESTIMATE SELECT count() FROM 03480_year_tbl WHERE toYear(d) = 2005)) AS same_index_scan,
    (SELECT rows FROM (EXPLAIN ESTIMATE SELECT count() FROM 03480_year_tbl WHERE year(d) = 2005))
  < (SELECT count() FROM 03480_year_tbl) AS pruned;
DROP TABLE 03480_year_tbl;

SELECT '-- year(<date>) is cacheable like toYear(<date>), year() is not';
DROP TABLE IF EXISTS 03480_year_qc;
CREATE TABLE 03480_year_qc (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO 03480_year_qc VALUES ('2020-05-05'), ('2020-06-06');
SELECT count() FROM 03480_year_qc WHERE toYear(d) = 2020 SETTINGS use_query_cache = 1;
SELECT count() FROM 03480_year_qc WHERE YEAR(d) = 2020 SETTINGS use_query_cache = 1;
SELECT year() SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
DROP TABLE 03480_year_qc;

SELECT '-- a pending mutation on year(<date>) does not break a read with apply_mutations_on_fly';
DROP TABLE IF EXISTS 03480_year_mut;
CREATE TABLE 03480_year_mut (d Date, v UInt32) ENGINE = MergeTree ORDER BY d;
INSERT INTO 03480_year_mut VALUES ('2020-05-05', 1), ('2021-06-06', 2);
SYSTEM STOP MERGES 03480_year_mut;
ALTER TABLE 03480_year_mut UPDATE v = v + 1 WHERE YEAR(d) = 2020 SETTINGS mutations_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '03480_year_mut' AND NOT is_done AND NOT is_killed;
SELECT sum(v) FROM 03480_year_mut SETTINGS apply_mutations_on_fly = 1;
DROP TABLE 03480_year_mut;
