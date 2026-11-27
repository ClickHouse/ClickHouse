-- Tests that query_cache_use_only_when_data_was_not_changed fails closed for data dependencies carried
-- by function arguments rather than by table expressions (issue #108713). The quoted spellings of
-- dictGet / joinGet and the bare-identifier spelling of `x IN table` never appear as table identifiers
-- in the query AST, so the set of referenced tables cannot be verified and the query cache must be
-- bypassed instead of serving a result that goes stale when only those sources change.

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache (which would require a no-parallel tag).

-- A Join table read through joinGet with a quoted table name.
DROP TABLE IF EXISTS j;
CREATE TABLE j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO j VALUES (1, 'old');
SELECT joinGet('j', 'v', toUInt64(1)) SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
TRUNCATE TABLE j;
INSERT INTO j VALUES (1, 'new');
-- The fresh value, not the stale cached one.
SELECT joinGet('j', 'v', toUInt64(1)) SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
DROP TABLE j;

-- A dictionary read through dictGet with a quoted dictionary name. The DIRECT layout reads the source
-- table on every lookup, so a change to the source is visible immediately, without a reload.
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS dict_source;
CREATE TABLE dict_source (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dict_source VALUES (1, 'old');
CREATE DICTIONARY dict (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dict_source')) LAYOUT(DIRECT());
SELECT dictGet('dict', 'v', toUInt64(1)) SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
TRUNCATE TABLE dict_source;
INSERT INTO dict_source VALUES (1, 'new');
-- The fresh value, not the stale cached one.
SELECT dictGet('dict', 'v', toUInt64(1)) SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
DROP DICTIONARY dict;
DROP TABLE dict_source;

-- A table read through `x IN table` spelled as a bare identifier.
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_in;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2);
CREATE TABLE t_in (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_in VALUES (1);
SELECT count() FROM t WHERE x IN t_in SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO t_in VALUES (2);
-- The fresh count, not the stale cached one: only the IN table changed, not the FROM table.
SELECT count() FROM t WHERE x IN t_in SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
DROP TABLE t;
DROP TABLE t_in;
