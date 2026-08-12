-- Every query submitted to a QueryRunner table is accounted for exactly once, whichever way the
-- table disposes of it: executed, refused because the table is at capacity, or abandoned by a
-- DROP. That accounting is what these arms assert: a synchronous INSERT and SYSTEM WAIT QUERY
-- RUNNER return only once every query they issued has retired, so a lost query hangs them. The
-- number that actually ran is bounded, not pinned, because the pool also refuses when the whole
-- server is saturated or when thread allocation is being fault injected. The exact counts, and
-- the thread pool occupancy, are asserted in tests/integration/test_query_runner.

SET send_logs_level = 'fatal';

-- The INSERT and SYSTEM WAIT return, so no submitted query was lost, and none ran twice.
CREATE TABLE dst (x UInt64) ENGINE = Memory;
CREATE TABLE runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO runner VALUES
    ('INSERT INTO dst VALUES (1)', {CLICKHOUSE_DATABASE:String}),
    ('INSERT INTO dst VALUES (2)', {CLICKHOUSE_DATABASE:String}),
    ('INSERT INTO dst VALUES (3)', {CLICKHOUSE_DATABASE:String}),
    ('INSERT INTO dst VALUES (4)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER runner;
SELECT count() <= 4 FROM dst;

-- A table accepts at most threads + max_queue_size queries at a time, here 1 + 1, and refuses the
-- surplus. Every refused query is still accounted for, which is what this arm asserts: the INSERT
-- returns and SYSTEM WAIT does not block, so no submission was lost. The count is bounded above
-- only. It has no lower bound because starting a thread also fails when the whole server's pool is
-- saturated, and every submission here needs a new thread, so all 32 can legitimately be refused.
-- test_capacity_bound in tests/integration/test_query_runner pins the accepted count exactly, on a
-- server no other test is using.
CREATE TABLE refused_dst (x UInt64) ENGINE = Memory;
CREATE TABLE tiny_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 1, max_queue_size = 1;
INSERT INTO tiny_runner SELECT 'INSERT INTO refused_dst SELECT sleep(1)', {CLICKHOUSE_DATABASE:String} FROM numbers(32);
SYSTEM WAIT QUERY RUNNER tiny_runner;
SELECT count() <= 1 + 1 FROM refused_dst;

-- A failing query does not disable the table: later submissions are still accepted and accounted
-- for. That they also run is pinned by test_failing_query_leaves_table_usable.
CREATE TABLE dst_after_failure (x UInt64) ENGINE = Memory;
CREATE TABLE failing_runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2;
INSERT INTO failing_runner VALUES ('SELECT throwIf(1)', {CLICKHOUSE_DATABASE:String});
INSERT INTO failing_runner VALUES
    ('INSERT INTO dst_after_failure VALUES (7)', {CLICKHOUSE_DATABASE:String}),
    ('INSERT INTO dst_after_failure VALUES (8)', {CLICKHOUSE_DATABASE:String}),
    ('INSERT INTO dst_after_failure VALUES (9)', {CLICKHOUSE_DATABASE:String}),
    ('INSERT INTO dst_after_failure VALUES (10)', {CLICKHOUSE_DATABASE:String});
SYSTEM WAIT QUERY RUNNER failing_runner;
SELECT count() <= 4 FROM dst_after_failure;

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
