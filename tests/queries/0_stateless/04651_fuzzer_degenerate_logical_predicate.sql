-- Tags: no-fasttest
-- no-fasttest: fast builds are `NDEBUG` without sanitizers, so `chassert` expands to
-- `(void)sizeof(...)` and every statement below would pass without exercising the abort.

-- Regression test: a degenerate logical call such as `and()` used to abort the fuzzer in
-- `QueryFuzzer::permutePredicateClause` (`chassert(!predicates.empty())`), because
-- `extractPredicates` flattened its empty argument list into nothing.
-- All four statements below aborted the server before the fix, between them reaching that
-- function through three callers: `addOrReplacePredicate`, the recursive call inside
-- `extractPredicates` (seeds 7 and 35), and the JOIN `ON` path.
-- `ast_fuzzer_runs = 0` keeps the server-side fuzzer from mutating the `fuzzQuery` arguments
-- (the Stress test profile sets it to 5), which would change the seed and the length cap.

SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 8) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and(and())', 3000, 7) LIMIT 300) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and(and())', 3000, 35) LIMIT 300) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT a.number FROM numbers(5) a INNER JOIN numbers(5) b ON and()', 3000, 1) LIMIT 300) SETTINGS ast_fuzzer_runs = 0;
