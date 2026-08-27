-- Tags: zookeeper, no-encrypted-storage
-- https://github.com/ClickHouse/ClickHouse/issues/72887
DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory();
BEGIN TRANSACTION;
EXPLAIN PLAN SELECT 1 FROM (SELECT 1) tx JOIN t0 ON TRUE; -- { serverError NOT_IMPLEMENTED }
ROLLBACK;
DROP TABLE IF EXISTS t0;
