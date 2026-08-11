-- A QueryRunner table occupies thread pool slots in proportion to its in-flight queries,
-- not in proportion to its 'threads' setting.

SET send_logs_level = 'fatal';

CREATE TABLE idle_runner (query String) ENGINE = QueryRunner SETTINGS threads = 1024;

-- Other tests may hold a few QueryRunner threads concurrently, so allow a margin.
-- Before the fix this reports 1024.
SELECT value < 512 FROM system.metrics WHERE metric = 'QueryRunnerThreads';

DROP TABLE idle_runner;

-- The engine still runs queries, and every submitted query is accounted for exactly once.
CREATE TABLE dst (x UInt64) ENGINE = Memory;
CREATE TABLE runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO runner VALUES ('INSERT INTO dst VALUES (1)', {CLICKHOUSE_DATABASE:String}), ('INSERT INTO dst VALUES (2)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER runner;
SELECT count() FROM dst;

-- More queries than the pool can hold at once: the refused ones are still accounted for,
-- so the INSERT returns and SYSTEM WAIT does not block forever.
CREATE TABLE tiny_runner (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 1, max_queue_size = 1;
INSERT INTO tiny_runner SELECT 'SELECT sleep(0.2)' FROM numbers(8);
SYSTEM WAIT QUERY RUNNER tiny_runner;
SELECT 'queue refusal accounted';

-- A failing query does not disable the table.
CREATE TABLE dst_after_failure (x UInt64) ENGINE = Memory;
CREATE TABLE failing_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO failing_runner VALUES ('SELECT throwIf(1)', {CLICKHOUSE_DATABASE:String});
INSERT INTO failing_runner VALUES ('INSERT INTO dst_after_failure VALUES (7)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER failing_runner;
SELECT count() FROM dst_after_failure;

-- Queries still queued when the table goes away are accounted for by the shutdown path.
CREATE TABLE dropped_runner (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 1, max_queue_size = 8;
INSERT INTO dropped_runner SELECT 'SELECT sleep(0.1)' FROM numbers(4);
DROP TABLE dropped_runner;
SELECT 'shutdown accounted';

DROP TABLE runner;
DROP TABLE tiny_runner;
DROP TABLE failing_runner;
DROP TABLE dst;
DROP TABLE dst_after_failure;
