-- Tags: no-fasttest, no-llvm-coverage
-- no-llvm-coverage: `fuzzQuery` drives the process-global `QueryFuzzer`, whose accumulated
-- state depends on what else ran in the same server, so its coverage of `QueryFuzzer.cpp`
-- varies run-to-run (same reason as `03834_fuzz_query_function` and
-- `04202_storage_fuzzquery_engine`).

-- Regression test: a degenerate logical call such as `and()` used to abort the fuzzer in
-- `QueryFuzzer::permutePredicateClause` (`chassert(!predicates.empty())`), because
-- `extractPredicates` flattened its empty argument list into nothing.
-- Every statement below aborted the server before the fix, each reaching that function through
-- a different caller: `addOrReplacePredicate`, the recursive call inside `extractPredicates`
-- (seeds 7 and 35), and the JOIN `ON` path.
-- `ast_fuzzer_runs = 0` keeps the server-side fuzzer from mutating the `fuzzQuery` arguments
-- (the Stress test profile sets it to 5), which would change the seed and the length cap.

SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 8) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and(and())', 3000, 7) LIMIT 300) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and(and())', 3000, 35) LIMIT 300) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT a.number FROM numbers(5) a INNER JOIN numbers(5) b ON and()', 3000, 1) LIMIT 300) SETTINGS ast_fuzzer_runs = 0;
