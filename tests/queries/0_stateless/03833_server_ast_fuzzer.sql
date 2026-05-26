-- Tags: no-fasttest, no-llvm-coverage
-- no-llvm-coverage: the global server-side AST fuzzer (`getGlobalASTFuzzer`) picks a
-- different set of AST mutations every run, which makes coverage of `QueryFuzzer.cpp`
-- and the parser/AST surface it touches non-deterministic.

-- Suppress error-level log messages from fuzzed queries that fail expectedly.
SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 3;

SELECT 1;

SELECT number FROM numbers(10) WHERE number > 5 ORDER BY number;
