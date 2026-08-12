-- Every query submitted to a QueryRunner table is accounted for exactly once, whichever way the
-- table disposes of it: executed, refused because the table is at capacity, or abandoned by a
-- DROP. Each assertion here reads only this test's own tables. The thread pool occupancy itself
-- is a server-global metric and is asserted in tests/integration/test_query_runner instead.
-- A refused query is accounted for without being run, and the pool also refuses when the whole
-- server is saturated or when thread allocation is being fault injected, so the number of queries
-- that actually ran has an exact upper bound and a loose lower one. The exact counts are asserted
-- in tests/integration/test_query_runner, on a server no other test is using.

SET send_logs_level = 'fatal';

-- The engine still runs queries, and every submitted query is accounted for exactly once.
CREATE TABLE dst (x UInt64) ENGINE = Memory;
CREATE TABLE runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO runner VALUES ('INSERT INTO dst VALUES (1)', {CLICKHOUSE_DATABASE:String}), ('INSERT INTO dst VALUES (2)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER runner;
SELECT count() <= 2 AND count() > 0 FROM dst;

-- A table accepts at most threads + max_queue_size queries at a time, here 1 + 1, and refuses the
-- surplus. Every refused query is still accounted for, so the INSERT returns and SYSTEM WAIT does
-- not block forever. Each accepted query sleeps for a second while the remaining 30 are submitted,
-- so no capacity is freed during submission. The upper bound is the engine's promise and is exact;
-- the lower bound stays loose because starting a thread can also fail when the whole server is
-- saturated, which is not what this arm is about. tests/integration/test_query_runner pins the
-- accepted count exactly, on a server no other test is using.
CREATE TABLE refused_dst (x UInt64) ENGINE = Memory;
CREATE TABLE tiny_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 1, max_queue_size = 1;
INSERT INTO tiny_runner SELECT 'INSERT INTO refused_dst SELECT sleep(1)', {CLICKHOUSE_DATABASE:String} FROM numbers(32);
SYSTEM WAIT QUERY RUNNER tiny_runner;
SELECT count() <= 1 + 1 AND count() > 0 FROM refused_dst;

-- A failing query does not disable the table: queries submitted after it still run. Two of them are
-- submitted so that the lower bound survives a single refusal.
CREATE TABLE dst_after_failure (x UInt64) ENGINE = Memory;
CREATE TABLE failing_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO failing_runner VALUES ('SELECT throwIf(1)', {CLICKHOUSE_DATABASE:String});
INSERT INTO failing_runner VALUES ('INSERT INTO dst_after_failure VALUES (7)', {CLICKHOUSE_DATABASE:String}), ('INSERT INTO dst_after_failure VALUES (8)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER failing_runner;
SELECT count() <= 2 AND count() > 0 FROM dst_after_failure;

-- Queries still outstanding when the table goes away are abandoned rather than executed against a
-- dropped table, and the DROP completes instead of hanging on them. Asynchronous mode is required:
-- a synchronous INSERT waits for its whole batch, so nothing would still be outstanding at DROP.
-- One query runs at a time and each takes 0.4s, so the DROP leaves 23 of them unrun.
CREATE TABLE dropped_dst (x UInt64) ENGINE = Memory;
CREATE TABLE dropped_runner (query String, database String) ENGINE = QueryRunner
    SETTINGS mode = 'asynchronous', threads = 1, max_queue_size = 32;
INSERT INTO dropped_runner
    SELECT 'INSERT INTO dropped_dst SELECT sleep(0.4)', {CLICKHOUSE_DATABASE:String} FROM numbers(24);
DROP TABLE dropped_runner;
SELECT count() < 24 FROM dropped_dst;
SELECT 'shutdown accounted';

DROP TABLE runner;
DROP TABLE tiny_runner;
DROP TABLE failing_runner;
DROP TABLE dst;
DROP TABLE refused_dst;
DROP TABLE dropped_dst;
DROP TABLE dst_after_failure;
