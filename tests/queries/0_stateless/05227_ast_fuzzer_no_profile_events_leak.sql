-- Tags: no-fasttest
-- no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

CREATE TABLE t_seed (x UInt64) ENGINE = MergeTree ORDER BY x;

SET ast_fuzzer_runs = 50, ast_fuzzer_any_query = 1;
INSERT INTO t_seed SELECT * FROM numbers(100);
SET ast_fuzzer_runs = 0;

SYSTEM FLUSH LOGS query_log;

-- A query that failed before start cannot have inserted rows. Fuzzed queries used to log
-- ExceptionBeforeStart with the ProfileEvents of the seed INSERT above.
SELECT count() FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionBeforeStart' AND ProfileEvents['InsertedRows'] > 0;
