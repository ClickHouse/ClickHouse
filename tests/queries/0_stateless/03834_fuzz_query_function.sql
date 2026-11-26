-- Tags: no-fasttest, no-llvm-coverage
-- no-llvm-coverage: `fuzzQuery` (gated by `allow_fuzz_query_functions`) mutates the AST
-- with the process-global `QueryFuzzer`, whose accumulated state depends on what other
-- queries ran before it in the same server. That makes the set of branches it touches
-- in `QueryFuzzer.cpp` non-deterministic, so this test is a source of flaky LLVM coverage.
SET allow_fuzz_query_functions = 1;
SELECT length(fuzzQuery('SELECT 1')) > 0;
SELECT fuzzQuery('not a valid query'); -- { serverError SYNTAX_ERROR }
