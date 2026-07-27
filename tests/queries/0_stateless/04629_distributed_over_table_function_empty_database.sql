-- Tags: no-replicated-database
-- Reason: one block creates a Distributed table over a table function that references a session-local
-- temporary table, which does not exist on the other replicas of a Replicated database (like 04603).

-- A table function target of a persisted `Distributed` table is bound to the current database at CREATE
-- time not only in its short forms: an explicit leading database argument that evaluates to an empty
-- string (`loop('', 'table')`, `merge('', 'regexp')`, `timeSeriesMetrics('', 'table')`, the `mergeTree*`
-- family) also resolves against the current database of the querying session at read time
-- (`evaluateConstantExpressionForDatabaseName`, `Context::resolveStorageID`), so it is folded and the
-- current database of the CREATE is baked into an empty result. Verified by querying from a session whose
-- current database is a different one holding decoy tables of the same name.

DROP TABLE IF EXISTS dist_merge_empty;
DROP TABLE IF EXISTS dist_loop_empty;
DROP TABLE IF EXISTS dist_loop_cd;
DROP TABLE IF EXISTS dist_mti_empty;
DROP TABLE IF EXISTS dist_mti_cd;
DROP TABLE IF EXISTS dist_mcbc_empty;
DROP TABLE IF EXISTS bind_db_src;
DROP VIEW IF EXISTS v_mti_empty;
DROP VIEW IF EXISTS v_mti_cd;
DROP VIEW IF EXISTS v_loop_cd;

CREATE TABLE bind_db_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_db_src VALUES (1), (2), (3);

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_db_src (n UInt64) ENGINE = MergeTree ORDER BY n;
-- Three separate inserts: three parts, so the decoy is distinguishable through `mergeTreeIndex` too.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_db_src VALUES (10);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_db_src VALUES (20);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_db_src VALUES (30);

-- merge('', 'regexp'): the empty database argument is frozen to the creating database.
CREATE TABLE dist_merge_empty ENGINE = Distributed(test_shard_localhost, merge('', '^bind_db_src$'));
SHOW CREATE TABLE dist_merge_empty;

-- loop('', 'table') is frozen the same way, and a `currentDatabase()` database argument is folded to its
-- CREATE-time value (the tables are not read here: `loop` repeats its source indefinitely).
CREATE TABLE dist_loop_empty ENGINE = Distributed(test_shard_localhost, loop('', 'bind_db_src'));
SHOW CREATE TABLE dist_loop_empty;
CREATE TABLE dist_loop_cd ENGINE = Distributed(test_shard_localhost, loop(currentDatabase(), 'bind_db_src'));
SHOW CREATE TABLE dist_loop_cd;

-- The `mergeTree*` family takes an always-explicit database argument, but an empty string still resolves
-- to the current database at read time, so it is frozen too.
CREATE TABLE dist_mti_empty (part_name String) ENGINE = Distributed(test_shard_localhost, mergeTreeIndex('', 'bind_db_src'));
SHOW CREATE TABLE dist_mti_empty;
CREATE TABLE dist_mcbc_empty (part_name String) ENGINE = Distributed(test_shard_localhost, mergeTreeCodecBlockCounts('', 'bind_db_src'));
SHOW CREATE TABLE dist_mcbc_empty;

-- The frozen explicit database also makes the referential dependency on the source table effective:
-- dropping the source is blocked while the `loop` / `mergeTreeIndex` targets reference it.
DROP TABLE bind_db_src SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- Read from a session bound to the decoy database: the targets keep reading the creating database
-- (sum 6, not 60; one part, not three), over both the local fast path and the serialized shard query.
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_merge_empty SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_merge_empty SETTINGS enable_analyzer = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_merge_empty SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_merge_empty SETTINGS enable_analyzer = 0, prefer_localhost_replica = 0;
SELECT count(DISTINCT part_name) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_mti_empty SETTINGS enable_analyzer = 1;
SELECT count(DISTINCT part_name) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_mti_empty SETTINGS enable_analyzer = 0;
SELECT count(DISTINCT part_name) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_mcbc_empty SETTINGS enable_analyzer = 1;
SELECT count(DISTINCT part_name) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_mcbc_empty SETTINGS enable_analyzer = 0;
USE {CLICKHOUSE_DATABASE:Identifier};

DROP TABLE dist_merge_empty;
DROP TABLE dist_loop_empty;
DROP TABLE dist_loop_cd;
DROP TABLE dist_mti_empty;

-- Discriminating check for `mergeTreeCodecBlockCounts`: with every other target gone, the dependency
-- recorded from its frozen database argument alone must still block dropping the source table.
DROP TABLE bind_db_src SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_mcbc_empty;

-- A `currentDatabase()` database argument is folded to a literal by the binder as well, and that literal -
-- not the expression - is what the dependency is taken from, so a `Distributed` target spelled that way
-- keeps blocking the drop of its source on its own (created here, with every other target already gone).
CREATE TABLE dist_mti_cd (part_name String) ENGINE = Distributed(test_shard_localhost, mergeTreeIndex(currentDatabase(), 'bind_db_src'));
SHOW CREATE TABLE dist_mti_cd;
DROP TABLE bind_db_src SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_mti_cd;

-- The `timeSeries*` / `prometheusQuery*` family resolves the pair through `Context::resolveStorageID`,
-- which looks up session-local temporary tables only when the database is empty - so the long form keeps
-- the temporary/external-table exemption the short form takes (see 04603): an empty database paired with
-- a table name that matches a temporary table stays as is, everything else is frozen.
SET allow_experimental_time_series_table = 1;
CREATE TABLE ts_bind ENGINE = TimeSeries;
CREATE TABLE dist_ts_empty
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics('', 'ts_bind'));
SHOW CREATE TABLE dist_ts_empty;
DROP TABLE dist_ts_empty;
DROP TABLE ts_bind;

CREATE TEMPORARY TABLE tmp_ts_bind ENGINE = TimeSeries;
CREATE TABLE dist_ts_tmp
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics('', 'tmp_ts_bind'));
SHOW CREATE TABLE dist_ts_tmp;
DROP TABLE dist_ts_tmp;
DROP TEMPORARY TABLE tmp_ts_bind;

-- Control for the dependency rule outside a Distributed target: a stored SELECT (a view) is never bound,
-- so its `mergeTreeIndex('', 'table')` still resolves per querying session and must NOT register a
-- create-time dependency - the source stays droppable under `check_referential_table_dependencies`.
-- The same holds for any other database expression that is not a name spelled out in the metadata:
-- `AddDefaultDatabaseVisitor` folds `currentDatabase()` only in DDL, so the `SELECT` of a view keeps it and
-- resolves it per querying session. Evaluating it while collecting dependencies would record the wrong
-- table - the one in the database of the `CREATE VIEW` - and leave the one actually read unprotected.
CREATE VIEW v_mti_empty AS SELECT part_name FROM mergeTreeIndex('', 'bind_db_src');
CREATE VIEW v_mti_cd AS SELECT part_name FROM mergeTreeIndex(currentDatabase(), 'bind_db_src');
CREATE VIEW v_loop_cd AS SELECT n FROM loop(currentDatabase(), 'bind_db_src');
DROP TABLE bind_db_src SETTINGS check_referential_table_dependencies = 1;
DROP VIEW v_mti_empty;
DROP VIEW v_mti_cd;
DROP VIEW v_loop_cd;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
