-- Tags: no-fasttest
-- no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 30;
SET ast_fuzzer_any_query = 1;
SET max_execution_time = 20;

-- The fuzzer inflates numeric literals, so numbers(100) can become a multi-million-row read.
-- Each seed carries SETTINGS that would lift the fuzzer's safety caps; the fix strips them from
-- the fuzzed query and bounds it, so the fuzzer cannot run away and the server stays responsive.

-- `= value` form: the override is parked in ASTSetQuery::changes.
SELECT sum(number) FROM numbers(100)
SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw', max_execution_time = 0, max_result_rows = 0;

-- `= DEFAULT` form: the override is parked in ASTSetQuery::default_settings; on re-parse it would
-- reset the pinned fuzz-context cap back to its unbounded default unless it is stripped too.
SELECT sum(number) FROM numbers(100)
SETTINGS max_rows_to_read = DEFAULT, read_overflow_mode = DEFAULT, max_execution_time = DEFAULT, max_result_rows = DEFAULT;

-- Repeated override: the parser appends one entry per occurrence, so a strip that removed only the
-- first copy would leave the second `max_rows_to_read = 0` to re-open the read cap on re-parse.
SELECT sum(number) FROM numbers(100)
SETTINGS max_rows_to_read = 0, max_rows_to_read = 0, read_overflow_mode = 'throw';

DROP TABLE IF EXISTS t_04344;
CREATE TABLE t_04344 (a UInt64, b String, c Array(UInt64)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04344 SELECT number, toString(number), range(number % 8) FROM numbers(100)
SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw';

-- Block-forming carrier: a trivial INSERT ... SELECT into a table that prefers large blocks copies
-- min_insert_block_size_rows into the SELECT's max_block_size, so a fuzzed numbers(100) inflated to
-- numbers(~1M) would push one ~1M-row block into the part writer in a single pipeline task before the
-- read cap can fire. The fix pins both block-forming settings small and strips the query's own
-- overrides, so this stays bounded even though the seed asks for a 1M-row block.
INSERT INTO t_04344 SELECT number, toString(number), range(number % 8) FROM numbers(100)
SETTINGS min_insert_block_size_rows = 1000000, max_block_size = 1000000, max_rows_to_read = 0;

-- CREATE storage-settings carrier: the cap is parked in ASTStorage::settings. After stripping the
-- only setting the node is empty; without pruning it ASTStorage::formatImpl emits a bare `SETTINGS`
-- and the fuzzed CREATE is skipped on re-parse.
DROP TABLE IF EXISTS t_04344_storage;
CREATE TABLE t_04344_storage (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS max_rows_to_read = 0;
DROP TABLE t_04344_storage;

-- BACKUP carrier: the cap lives in ASTBackupQuery::settings, outside the AST `children`, so a
-- `children`-only strip walk would miss it and the override would survive on re-parse.
BACKUP TABLE t_04344 TO Null SETTINGS max_execution_time = 0 FORMAT Null;

SELECT 1;

DROP TABLE t_04344;
