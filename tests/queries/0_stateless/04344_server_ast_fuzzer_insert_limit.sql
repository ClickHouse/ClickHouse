-- Tags: no-fasttest
-- no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

-- The server-side AST fuzzer runs fuzzed queries as a finish callback of the outer query.
-- QueryFuzzer rewrites numeric literals to boundary values (1 MiB +/- 1, INT_MAX, ...), so a
-- seed numbers(100) can become numbers(1048576) and the fuzzed INSERT reads millions of rows;
-- a single in-flight fuzzed query has no cancel callback, so it ignores the outer KILL/timeout
-- and runs for minutes under sanitizers, tripping the stress hung check. The fix bounds the
-- fuzzed read (max_rows_to_read + read_overflow_mode=break) and cancels in-flight fuzzed queries
-- on the outer deadline. This test just exercises that path and confirms the server stays
-- responsive afterwards (no hang, no crash).

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 30;
SET ast_fuzzer_any_query = 1;
SET max_execution_time = 20;

DROP TABLE IF EXISTS t_04344;
CREATE TABLE t_04344 (a UInt64, b String, c Array(UInt64)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04344 SELECT number, toString(number), range(number % 8) FROM numbers(100);

SELECT 1;

DROP TABLE t_04344;
