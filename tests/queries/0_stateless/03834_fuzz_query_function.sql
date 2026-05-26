-- Tags: no-fasttest, no-llvm-coverage
-- no-llvm-coverage: `fuzzQuery('SELECT 1')` is invoked without an explicit seed, so it
-- defaults to `randomSeed`, producing different fuzzed AST per run and a different
-- coverage footprint inside `QueryFuzzer.cpp`.
SET allow_fuzz_query_functions = 1;
SELECT length(fuzzQuery('SELECT 1')) > 0;
SELECT fuzzQuery('not a valid query'); -- { serverError SYNTAX_ERROR }
