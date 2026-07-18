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

-- `test_cluster_multiple_nodes_all_unavailable` has no local replicas: the target table function runs only
-- on the (unavailable) remote shards.

-- Over such a cluster, a persisted `Distributed` table with an explicit column list must be creatable
-- without resolving the target table function locally. `timeSeries*` / `prometheus*` resolve their source
-- storage in `parseArguments` (`TableFunctionTimeSeriesTarget::parseArguments` -> `getTargetTable`), so
-- requiring the source to exist on a node that only holds the metadata would make the target impossible to
-- create there. The source `ts_absent` deliberately does not exist locally: the create must still succeed
-- because the argument resolution is deferred to a read on the shards.
CREATE TABLE dist_ts_remote_only
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, timeSeriesMetrics('ts_absent'));
SHOW CREATE TABLE dist_ts_remote_only;
DROP TABLE dist_ts_remote_only;

-- `remote()` / `cluster()` with the target given by a factory alias (`timeSeriesData` is an alias of
-- `timeSeriesSamples`) must not record a local dependency on the table the function reads only on the remote
-- shards. Detecting the table function with `KnownTableFunctionNames` (which does not contain factory aliases)
-- missed the alias and let the generic dependency walk record a bogus dependency on `ts_dep`, which then
-- blocked its DROP / RENAME under `check_referential_table_dependencies = 1`. `remote()` is always treated by
-- the dependency visitor as having no local replicas, so the target runs remotely and its source is not a
-- local dependency (`remote('127.0.0.1', ...)` reaches this server, so the view's columns can still be
-- inferred).
CREATE TABLE ts_dep ENGINE = TimeSeries;
CREATE VIEW v_ts_remote AS SELECT * FROM remote('127.0.0.1', timeSeriesData(ts_dep));
SET check_referential_table_dependencies = 1;
DROP TABLE ts_dep;
SET check_referential_table_dependencies = 0;
DROP VIEW v_ts_remote;

-- Control: over `cluster('test_shard_localhost', ...)` the function reads `ts_dep2` on the local replica, so
-- the dependency IS recorded and the DROP is rejected.
CREATE TABLE ts_dep2 ENGINE = TimeSeries;
CREATE VIEW v_ts_local AS SELECT * FROM cluster('test_shard_localhost', timeSeriesData(ts_dep2));
SET check_referential_table_dependencies = 1;
DROP TABLE ts_dep2; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_referential_table_dependencies = 0;
DROP VIEW v_ts_local;
DROP TABLE ts_dep2;
