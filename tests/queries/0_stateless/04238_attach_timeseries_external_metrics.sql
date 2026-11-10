-- Tags: no-replicated-database
-- ^^ The experimental TimeSeries table engine does not round-trip through
-- DatabaseReplicated; DETACH/ATTACH of the metrics target hangs the cleanup
-- query (same reason as 04131_prometheus_query_parser).
--
-- Regression test for the `StorageTimeSeries` constructor: when the external
-- metrics target table is not loaded at ATTACH time (server startup ordering),
-- the constructor used to dereference a null pointer from `tryGetTable` and
-- abort the server under UBSan.
--
-- Reproduces the ordering by detaching both tables and then re-attaching the
-- `TimeSeries` table before its metrics target.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS metrics_target_04238;
DROP TABLE IF EXISTS ts_with_external_metrics_04238;

CREATE TABLE metrics_target_04238 (
    metric_family_name String,
    type String,
    unit String,
    help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts_with_external_metrics_04238 ENGINE = TimeSeries METRICS metrics_target_04238;

DETACH TABLE ts_with_external_metrics_04238;
DETACH TABLE metrics_target_04238;

-- This is the regression path: attaching the TimeSeries table while its external
-- metrics target is still detached must not crash and must not throw.
ATTACH TABLE ts_with_external_metrics_04238;
ATTACH TABLE metrics_target_04238;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'ts_with_external_metrics_04238';

DROP TABLE ts_with_external_metrics_04238;
DROP TABLE metrics_target_04238;
