-- Tags: need-query-parameters

-- Tests that the modification-hash consistency machinery (issue #108713) fails closed on constructs
-- whose result can change without any referenced table changing (PR #108721 review):
--   1. Non-deterministic functions: an unchanged set of referenced tables is not proof that the result
--      is unchanged when the query calls `now64`, `rand`, etc.
--   2. View-like storages without a real UUID (an `Ordinary` database): a DROP + CREATE resets the
--      per-lifetime metadata version, so a definition change that keeps the same columns and stored
--      SELECT (e.g. `SQL SECURITY INVOKER` -> `DEFINER`) could repeat an earlier hash.
--   3. Re-creating a view in an `Atomic` database is a new incarnation (new UUID), so its hash never
--      repeats the earlier one even when the definition is identical.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2);

-- 1a. A view whose stored SELECT calls a non-deterministic function cannot prove its result unchanged.
DROP TABLE IF EXISTS v_det;
DROP TABLE IF EXISTS v_nondet;
CREATE VIEW v_det AS SELECT count() FROM t;
CREATE VIEW v_nondet AS SELECT now64(9), count() FROM t;
SELECT 'deterministic view not null', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 'v_det';
SELECT 'non-deterministic view null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'v_nondet';

-- 1b. The query cache with the consistency setting bypasses a non-deterministic query - no entry is
-- stored - even when `query_cache_nondeterministic_function_handling = 'save'` would allow caching it.
-- Without the consistency setting the same query is stored (the control).
-- `system.query_cache` is server-wide and its entries outlive a single test run (`query_cache_ttl`
-- defaults to 60 seconds), so both lookups below must be immune to entries of a concurrent or earlier
-- run of this very test: the two marker literals are chosen so that neither is a substring of the
-- other, and the current database name is folded into the cached queries (the predicate is a no-op for
-- the result, and the query parameter is substituted before the query text is stored), which makes
-- every run's entries distinguishable.
SELECT count(), 'qc_04759_nondet' FROM t WHERE rand() >= 0 AND {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
SELECT 'non-deterministic query not stored', count() = 0 FROM system.query_cache WHERE query LIKE '%qc_04759_nondet%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';
SELECT count(), 'qc_04759_control' FROM t WHERE rand() >= 0 AND {CLICKHOUSE_DATABASE:String} != '' SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_nondeterministic_function_handling = 'save';
SELECT 'control query stored', count() > 0 FROM system.query_cache WHERE query LIKE '%qc_04759_control%' AND query LIKE '%' || currentDatabase() || '%' AND query NOT LIKE '%system.query_cache%';

-- 2. A view and a materialized view in an `Ordinary` database have no UUID and fail closed, even over
-- a table (in the test's `Atomic` database) that reports a hash itself.
SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v_ord AS SELECT x FROM t;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ord ENGINE = Memory AS SELECT x FROM t;
SELECT 'ordinary view null', modification_hash IS NULL FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'v_ord';
SELECT 'ordinary materialized view null', modification_hash IS NULL FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'mv_ord';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- 3. Re-creating a view with the same definition in an `Atomic` database changes the hash (new UUID,
-- new incarnation), so a definition change that keeps the same columns and stored SELECT - such as a
-- different `SQL SECURITY` - can never repeat the earlier incarnation's hash.
DROP TABLE IF EXISTS hashes_04759;
CREATE TABLE hashes_04759 (name String, hash UInt128) ENGINE = Memory;
DROP TABLE IF EXISTS v_sec;
CREATE VIEW v_sec SQL SECURITY INVOKER AS SELECT x FROM t;
INSERT INTO hashes_04759 SELECT 'invoker', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v_sec';
DROP TABLE v_sec;
CREATE VIEW v_sec SQL SECURITY DEFINER AS SELECT x FROM t;
INSERT INTO hashes_04759 SELECT 'definer', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v_sec';
SELECT 'recreated view hash changed', (SELECT hash FROM hashes_04759 WHERE name = 'invoker') != (SELECT hash FROM hashes_04759 WHERE name = 'definer');

DROP TABLE v_sec;
DROP TABLE hashes_04759;
DROP TABLE v_nondet;
DROP TABLE v_det;
DROP TABLE t;
