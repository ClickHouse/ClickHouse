-- Tags: no-fasttest
-- no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 30;
SET ast_fuzzer_any_query = 1;
SET max_execution_time = 20;

-- The fuzzer inflates numeric literals, so numbers(100) can become a multi-million-row read.
-- Each seed carries SETTINGS that would lift the fuzzer's safety caps; the fix strips them from
-- the fuzzed query and bounds it, so the fuzzer cannot run away and the server stays responsive.

SELECT sum(number) FROM numbers(100)
SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw', max_execution_time = 0, max_result_rows = 0;

DROP TABLE IF EXISTS t_04344;
CREATE TABLE t_04344 (a UInt64, b String, c Array(UInt64)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04344 SELECT number, toString(number), range(number % 8) FROM numbers(100)
SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw';

SELECT 1;

DROP TABLE t_04344;
