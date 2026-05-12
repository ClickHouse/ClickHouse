-- Tags: no-fasttest, no-stress
-- ^ no-stress: the Stress test wraps every query with the server-side AST fuzzer.
--   The fuzzer mutates the `randomString` length / `numbers` arguments into very large values,
--   and `base32Encode` / `base58Encode` perform per-row encoding without `isCancelled` checks,
--   so the `max_execution_time=10` cap inside `executeASTFuzzerQueries` cannot stop the worker
--   thread, triggering the hung-check.

SELECT base32Encode(randomString(1, 100)) FROM numbers(1000) FORMAT Null;
SELECT base58Encode(randomString(1, 100)) FROM numbers(1000) FORMAT Null;