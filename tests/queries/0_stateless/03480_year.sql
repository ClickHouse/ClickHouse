-- Tags: no-parallel
-- Tag no-parallel: uses the query result cache, which is a global singleton.

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

-- The determinism check matches by function name before overload resolution, so the argument form
-- year(<date>) counts as non-deterministic too; use toYear(<date>) where determinism is required.
SELECT '-- query result cache: year(<date>) is non-deterministic (not cached), unlike toYear(<date>)';
SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS 03480_qc;
CREATE TABLE 03480_qc (ts DateTime) ENGINE = MergeTree ORDER BY ts;
INSERT INTO 03480_qc VALUES ('2024-06-01 00:00:00'), ('2023-01-01 00:00:00');

-- The check runs on the initiator, so pin enable_parallel_replicas = 0.
SELECT '-- toYear(<date>) is deterministic and caches';
SELECT count() FROM 03480_qc WHERE toYear(ts) = 2024 SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw', enable_parallel_replicas = 0;
SELECT '-- year(<date>) is rejected from the query cache';
SELECT count() FROM 03480_qc WHERE year(ts) = 2024 SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw', enable_parallel_replicas = 0; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT '-- niladic year() is rejected from the query cache too';
SELECT year() SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT '-- only the deterministic toYear(<date>) query is cached';
SELECT count() FROM system.query_cache WHERE query LIKE '%03480_qc%';

DROP TABLE 03480_qc;
SYSTEM DROP QUERY CACHE;
