-- Regression test: `QueryFuzzer::getRandomType` composed a `Map` whose key was drawn without
-- validation, so `DataTypeMap`'s constructor rejected the fuzzer's own generated type with
-- `BAD_ARGUMENTS` and the whole mutation was aborted. `fuzzDataType` already guarded its `Map`
-- key with the same predicate; `getRandomType` did not.
-- One seed per half of `DataTypeMap::isValidKeyType`: seed 81 drew a `Nullable` key, seed 146 a
-- `LowCardinality(Nullable)` one.
-- `ast_fuzzer_runs = 0` keeps the server-side fuzzer from mutating the `fuzzQuery` arguments
-- (the Stress test profile sets it to 5), which would change the seed and the length cap.

SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 81) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 146) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;
