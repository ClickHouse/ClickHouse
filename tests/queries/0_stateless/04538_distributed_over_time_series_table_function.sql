-- Tags: no-fasttest, no-replicated-database
-- ^ no-fasttest: `prometheusQuery` parses PromQL via ANTLR4, which is disabled in the Fast test build
--   (same as the other prometheus/TimeSeries tests). TimeSeries tables are not supported in Replicated databases.

-- The short forms of the TimeSeries table functions (`timeSeriesMetrics` / `timeSeriesData` /
-- `timeSeriesTags` / `timeSeriesSelector` / `prometheusQuery` / `prometheusQueryRange`) resolve an omitted
-- database against the current database at query time. When such a function is the persisted target of a
-- `Distributed` table, the database is bound at CREATE time, so queries read the same TimeSeries table
-- regardless of the current database of the querying session, and the named table is a referential
-- dependency of the `Distributed` table.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_src;
CREATE TABLE ts_src ENGINE = TimeSeries;

-- String and identifier short forms are both bound to the creating database.
CREATE TABLE dist_ts_metrics ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics('ts_src'));
SHOW CREATE TABLE dist_ts_metrics;
CREATE TABLE dist_ts_tags ENGINE = Distributed(test_shard_localhost, timeSeriesTags(ts_src));
SHOW CREATE TABLE dist_ts_tags;
CREATE TABLE dist_pq ENGINE = Distributed(test_shard_localhost, prometheusQuery('ts_src', 'up', now()));
SHOW CREATE TABLE dist_pq;

-- Queried from a different current database (which has no `ts_src`), the tables still read the TimeSeries
-- table of the database they were created in; before the binding this failed to resolve `ts_src`.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_ts_metrics;
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_ts_tags;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The TimeSeries table named by the persisted targets is a referential dependency of the `Distributed`
-- tables: with `check_referential_table_dependencies = 1` it cannot be dropped or renamed away from under
-- them.
SET check_referential_table_dependencies = 1;
DROP TABLE ts_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
RENAME TABLE ts_src TO ts_src2; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_referential_table_dependencies = 0;

DROP TABLE dist_pq;
DROP TABLE dist_ts_tags;
DROP TABLE dist_ts_metrics;
DROP TABLE ts_src;
