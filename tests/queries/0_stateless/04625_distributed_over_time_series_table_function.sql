-- The one-argument string form of the `timeSeries*` table functions takes the whole string as a table
-- name of the current database; it is never split at a dot (`timeSeriesMetrics('a.b')` reads the table
-- literally named `a.b`, not `a`.`b`). A `Distributed` table persisted over such a target must keep
-- exactly these semantics: the bound form reads the same table the direct call reads.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS `dot.target`;
DROP TABLE IF EXISTS dist_ts_dot;
DROP TABLE IF EXISTS dist_ts_qualified;
DROP TABLE IF EXISTS v_ts_dot;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS v_ts_plain;
DROP TABLE IF EXISTS ts_shadow;
DROP TABLE IF EXISTS dist_ts_tmp;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

-- A table whose name literally contains a dot, in the current database.
CREATE TABLE `dot.target` ENGINE = TimeSeries;
SELECT count() FROM timeSeriesMetrics('dot.target');

-- The persisted target binds the current database explicitly and keeps the whole string as the table name.
CREATE TABLE dist_ts_dot ENGINE = Distributed('test_shard_localhost', timeSeriesMetrics('dot.target'));
SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_ts_dot';
SELECT count() FROM dist_ts_dot;
DROP TABLE dist_ts_dot;

-- The raw (unrewritten) one-string form of a stored query resolves against the current database and the
-- session temporary tables of the querying session at execution time, so it registers no referential
-- dependency: even with `check_referential_table_dependencies = 1` the dotted-name table can be dropped
-- (it is re-created right away for the rest of the test). The target persisted by `Distributed` is bound
-- to an explicit database at CREATE time and therefore does register one.
CREATE VIEW v_ts_dot AS SELECT * FROM timeSeriesMetrics('dot.target');
SET check_referential_table_dependencies = 1;
DROP TABLE `dot.target`;
DROP TABLE v_ts_dot;
CREATE TABLE `dot.target` ENGINE = TimeSeries;
CREATE TABLE dist_ts_dot ENGINE = Distributed('test_shard_localhost', timeSeriesMetrics('dot.target'));
DROP TABLE `dot.target`; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_ts_dot;

-- A dotted string naming a table of another database references nothing: the direct call and the CREATE
-- of a Distributed table over it fail the same way (the string is looked up as a whole name in the
-- current database and must not be silently rewritten to the other database's table).
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_target ENGINE = TimeSeries;
SELECT count() FROM timeSeriesMetrics({CLICKHOUSE_DATABASE_1:String} || '.ts_target'); -- { serverError UNKNOWN_TABLE }
CREATE TABLE dist_ts_qualified ENGINE = Distributed('test_shard_localhost', timeSeriesMetrics({CLICKHOUSE_DATABASE_1:String} || '.ts_target')); -- { serverError UNKNOWN_TABLE }

-- The identifier form is the one that supports qualification, and a qualified identifier is persisted as is.
SELECT count() FROM timeSeriesMetrics({CLICKHOUSE_DATABASE_1:Identifier}.ts_target);
CREATE TABLE dist_ts_qualified ENGINE = Distributed('test_shard_localhost', timeSeriesMetrics({CLICKHOUSE_DATABASE_1:Identifier}.ts_target));
SELECT count() FROM dist_ts_qualified;
DROP TABLE dist_ts_qualified;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_target;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE `dot.target`;

-- An unqualified identifier in a stored query outside `Distributed` is likewise resolved by the querying
-- session at execution time, so it registers no referential dependency (the DROP of the named table is not
-- blocked). The opposite case — a database-qualified identifier names a stable object and does register
-- one — lives in 04626_time_series_qualified_name_dependency.sh: the qualifying database needs a
-- per-run-unique literal name in the view body, which a .sql test cannot produce (a `{...:Identifier}`
-- parameter inside a view body is persisted unsubstituted, and a fixed name collides between concurrent
-- runs in the flaky check).
CREATE TABLE ts_plain ENGINE = TimeSeries;
CREATE VIEW v_ts_plain AS SELECT * FROM timeSeriesMetrics(ts_plain);
DROP TABLE ts_plain;
DROP VIEW v_ts_plain;

-- The only unqualified spelling that survives the binding of a persisted `Distributed` target refers to a
-- session temporary table, which takes no part in dependency tracking: no dependency is registered on a
-- same-named permanent table, so the latter can be dropped.
CREATE TABLE ts_shadow ENGINE = TimeSeries;
CREATE TEMPORARY TABLE ts_shadow ENGINE = TimeSeries;
CREATE TABLE dist_ts_tmp (metric_family_name String) ENGINE = Distributed('test_shard_localhost', timeSeriesMetrics(ts_shadow));
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.ts_shadow;
DROP TABLE dist_ts_tmp;
DROP TEMPORARY TABLE ts_shadow;
