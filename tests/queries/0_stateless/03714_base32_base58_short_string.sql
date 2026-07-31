-- Tags: no-fasttest

-- The queries below disable the server-side AST fuzzer (`ast_fuzzer_runs = 0`):
-- the Stress test wraps every query with it, and it mutates the `randomString` length /
-- `numbers` arguments into very large values, and `base32Encode` / `base58Encode` perform
-- per-row encoding without `isCancelled` checks, so the `max_execution_time` cap inside
-- `executeASTFuzzerQueries` cannot stop the worker thread, triggering the hung-check.

SELECT base32Encode(randomString(1, 100)) FROM numbers(1000) SETTINGS ast_fuzzer_runs = 0 FORMAT Null;
SELECT base58Encode(randomString(1, 100)) FROM numbers(1000) SETTINGS ast_fuzzer_runs = 0 FORMAT Null;
