-- Tags: no-replicated-database
-- Coverage for StorageTimeSeries::initTarget called via ATTACH.
-- The ATTACH path in initTarget() is distinct from CREATE and exercises
-- different branches that are otherwise uncovered.
-- Note: ATTACH TABLE with TimeSeries hangs in DatabaseReplicated mode
-- because DDL goes through the replicated log requiring replica sync.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_attach_test;

CREATE TABLE ts_attach_test ENGINE = TimeSeries FORMAT Null;

DETACH TABLE ts_attach_test;

SELECT count() FROM system.tables WHERE name = 'ts_attach_test' AND database = currentDatabase();

ATTACH TABLE ts_attach_test;

SELECT count() FROM system.tables WHERE name = 'ts_attach_test' AND database = currentDatabase();

DROP TABLE ts_attach_test;