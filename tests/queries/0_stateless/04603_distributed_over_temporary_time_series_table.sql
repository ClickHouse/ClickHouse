-- Tags: no-replicated-database
-- Reason: creates a Distributed table over a table function that references a session-local temporary
-- table, which does not exist on the other replicas of a Replicated database.

-- Binding a Distributed table function target to the current database at CREATE time keeps the
-- temporary/external-table exemption that plain table identifiers already have: the short forms of the
-- `timeSeries*` / `prometheusQuery*` family name a table resolved through `Context::resolveStorageID`
-- (temporary and external tables are looked up before the current database), so an unqualified name that
-- matches a session-local temporary table must NOT be rewritten to the current database - that would shadow
-- the temporary table with a permanent one of the same name.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS dist_over_tmp_ts;

CREATE TEMPORARY TABLE ts_src ENGINE = TimeSeries;

-- The identifier short form of the target names the temporary table, so it stays unqualified in the stored
-- definition. The Distributed table has an explicit column list, so the target is not resolved at CREATE time.
CREATE TABLE dist_over_tmp_ts
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics(ts_src));
SHOW CREATE TABLE dist_over_tmp_ts;
DROP TABLE dist_over_tmp_ts;

-- The string-literal short form is exempted the same way.
CREATE TABLE dist_over_tmp_ts
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics('ts_src'));
SHOW CREATE TABLE dist_over_tmp_ts;
-- The table is intentionally not read here: a session-local temporary table only exists on the initiator, so
-- it is visible to the target table function only when the shard query runs on the local replica. Reading it
-- over the network path (a remote replica, or the parallel-replicas cluster) would raise `UNKNOWN_TABLE`.
-- Keeping the unqualified name in the stored definition (asserted above) is the property under test.
DROP TABLE dist_over_tmp_ts;
DROP TEMPORARY TABLE ts_src;

-- Control: a permanent TimeSeries table of the same name IS qualified with the current database.
CREATE TABLE ts_src ENGINE = TimeSeries;
CREATE TABLE dist_over_tmp_ts
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = Distributed(test_shard_localhost, timeSeriesMetrics(ts_src));
SHOW CREATE TABLE dist_over_tmp_ts;
DROP TABLE dist_over_tmp_ts;
DROP TABLE ts_src;
