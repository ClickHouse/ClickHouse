-- Tests that the query cache safety gate (`checkCanWriteQueryResultCache`) inspects only the
-- `additional_table_filters` entries that can apply to a table the query reads. An entry keyed by an
-- unrelated table never runs, so a non-deterministic function or a system table inside it must not
-- make the query throw or become uncachable. The tables use engines that read only their own data
-- (`MergeTree`, `Log`, `StripeLog`, `Memory`), so the set of names an entry can match is known.

DROP TABLE IF EXISTS t_05341_mt;
DROP TABLE IF EXISTS t_05341_log;
DROP TABLE IF EXISTS t_05341_stripelog;
DROP TABLE IF EXISTS t_05341_memory;
CREATE TABLE t_05341_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_05341_log (x UInt64) ENGINE = Log;
CREATE TABLE t_05341_stripelog (x UInt64) ENGINE = StripeLog;
CREATE TABLE t_05341_memory (x UInt64) ENGINE = Memory;
INSERT INTO t_05341_mt VALUES (1), (2), (3);
INSERT INTO t_05341_log VALUES (1), (2), (3);
INSERT INTO t_05341_stripelog VALUES (1), (2), (3);
INSERT INTO t_05341_memory VALUES (1), (2), (3);

SELECT 'non-deterministic filter of an unrelated table';
SELECT count() FROM t_05341_mt SETTINGS additional_table_filters = {'t_05341_other': 'rand() >= 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05341_mt';
SELECT count() > 0 FROM system.query_cache WHERE tag = '05341_mt';
SELECT count() FROM t_05341_log SETTINGS additional_table_filters = {'t_05341_other': 'rand() >= 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05341_log';
SELECT count() > 0 FROM system.query_cache WHERE tag = '05341_log';
SELECT count() FROM t_05341_stripelog SETTINGS additional_table_filters = {'t_05341_other': 'rand() >= 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05341_stripelog';
SELECT count() > 0 FROM system.query_cache WHERE tag = '05341_stripelog';

SELECT 'system table in a filter of an unrelated table';
SELECT count() FROM t_05341_memory SETTINGS additional_table_filters = {'t_05341_other': 'x IN (SELECT 1 FROM system.one)'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05341_memory';
SELECT count() > 0 FROM system.query_cache WHERE tag = '05341_memory';

-- An entry keyed by the table the query reads is still checked.
SELECT 'non-deterministic filter of the table read';
SELECT count() FROM t_05341_log SETTINGS additional_table_filters = {'t_05341_log': 'x <= 100 + rand() * 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05341_log_throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() FROM system.query_cache WHERE tag = '05341_log_throw';

DROP TABLE t_05341_mt;
DROP TABLE t_05341_log;
DROP TABLE t_05341_stripelog;
DROP TABLE t_05341_memory;
