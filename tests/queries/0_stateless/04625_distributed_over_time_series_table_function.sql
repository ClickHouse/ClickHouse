-- The one-argument string form of the `timeSeries*` table functions takes the whole string as a table
-- name of the current database; it is never split at a dot (`timeSeriesMetrics('a.b')` reads the table
-- literally named `a.b`, not `a`.`b`). A `Distributed` table persisted over such a target must keep
-- exactly these semantics: the bound form reads the same table the direct call reads.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS `dot.target`;
DROP TABLE IF EXISTS dist_ts_dot;
DROP TABLE IF EXISTS dist_ts_qualified;
DROP TABLE IF EXISTS v_ts_dot;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

-- A table whose name literally contains a dot, in the current database.
CREATE TABLE `dot.target` ENGINE = TimeSeries;
SELECT count() FROM timeSeriesMetrics('dot.target');

-- The persisted target binds the current database explicitly and keeps the whole string as the table name.
CREATE TABLE dist_ts_dot ENGINE = Distributed('test_shard_localhost', timeSeriesMetrics('dot.target'));
SELECT replaceAll(create_table_query, currentDatabase(), 'default') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_ts_dot';
SELECT count() FROM dist_ts_dot;
DROP TABLE dist_ts_dot;

-- The referential dependency of the raw (unrewritten) one-string form points at the table the function
-- actually reads - the dotted name in the current database, not `dot`.`target` - so it protects that
-- table from being dropped out from under the reader.
CREATE VIEW v_ts_dot AS SELECT * FROM timeSeriesMetrics('dot.target');
SET check_referential_table_dependencies = 1;
DROP TABLE `dot.target`; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE v_ts_dot;

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
