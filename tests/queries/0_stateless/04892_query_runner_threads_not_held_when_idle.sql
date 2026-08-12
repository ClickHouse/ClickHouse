-- A QueryRunner table occupies thread pool slots in proportion to its in-flight queries,
-- not in proportion to its 'threads' setting.

SET send_logs_level = 'fatal';

-- An idle table must contribute exactly zero threads. Measured as a delta so that QueryRunner
-- tables belonging to other tests running in parallel can neither mask a regression nor cause one.
-- Before the fix this delta is 1024. A negative delta only means a neighbour finished in between,
-- which cannot hide a regression here because a regression is strictly positive.
CREATE TABLE threads_before (value Int64) ENGINE = Memory;
INSERT INTO threads_before SELECT value FROM system.metrics WHERE metric = 'QueryRunnerThreads';
CREATE TABLE idle_runner (query String) ENGINE = QueryRunner SETTINGS threads = 1024;
SELECT (SELECT value FROM system.metrics WHERE metric = 'QueryRunnerThreads')
     - (SELECT value FROM threads_before) <= 0;
DROP TABLE idle_runner;

-- The engine still runs queries, and every submitted query is accounted for exactly once.
CREATE TABLE dst (x UInt64) ENGINE = Memory;
CREATE TABLE runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO runner VALUES ('INSERT INTO dst VALUES (1)', {CLICKHOUSE_DATABASE:String}), ('INSERT INTO dst VALUES (2)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER runner;
SELECT count() FROM dst;

-- More queries than the capacity bound (threads + max_queue_size) allows: the surplus is refused,
-- and every refused query is still accounted for, so the INSERT returns and SYSTEM WAIT does not
-- block forever. An unlimited queue would let all 32 through and redden this.
CREATE TABLE refused_dst (x UInt64) ENGINE = Memory;
CREATE TABLE tiny_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 1, max_queue_size = 1;
INSERT INTO tiny_runner SELECT 'INSERT INTO refused_dst VALUES (1)', {CLICKHOUSE_DATABASE:String} FROM numbers(32);
SYSTEM WAIT QUERY RUNNER tiny_runner;
SELECT count() < 32 AND count() > 0 FROM refused_dst;

-- A failing query does not disable the table.
CREATE TABLE dst_after_failure (x UInt64) ENGINE = Memory;
CREATE TABLE failing_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO failing_runner VALUES ('SELECT throwIf(1)', {CLICKHOUSE_DATABASE:String});
INSERT INTO failing_runner VALUES ('INSERT INTO dst_after_failure VALUES (7)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER failing_runner;
SELECT count() FROM dst_after_failure;

-- Queries still outstanding when the table goes away are abandoned rather than executed against a
-- dropped table, and the DROP completes instead of hanging on them. Asynchronous mode is required:
-- a synchronous INSERT waits for its whole batch, so nothing would still be outstanding at DROP.
CREATE TABLE dropped_dst (x UInt64) ENGINE = Memory;
CREATE TABLE dropped_runner (query String, database String) ENGINE = QueryRunner
    SETTINGS mode = 'asynchronous', threads = 1, max_queue_size = 32;
INSERT INTO dropped_runner
    SELECT 'INSERT INTO dropped_dst SELECT sleep(0.4)', {CLICKHOUSE_DATABASE:String} FROM numbers(24);
-- Most jobs are still outstanding: one runs at a time and each takes 0.4s. Proven, not assumed.
SELECT value > 1 FROM system.metrics WHERE metric = 'QueryRunnerThreadsScheduled';
DROP TABLE dropped_runner;
-- The queries abandoned by the DROP did not run, so far fewer than 24 rows landed.
SELECT count() < 24 FROM dropped_dst;
SELECT 'shutdown accounted';

DROP TABLE runner;
DROP TABLE tiny_runner;
DROP TABLE failing_runner;
DROP TABLE dst;
DROP TABLE refused_dst;
DROP TABLE dropped_dst;
DROP TABLE dst_after_failure;
DROP TABLE threads_before;
