-- Regression test: `QueryFuzzer::getRandomType` composed a `Map` whose key was drawn without
-- validation, so `DataTypeMap`'s constructor rejected the fuzzer's own generated type with
-- `BAD_ARGUMENTS` and the whole mutation was aborted. `fuzzDataType` already guarded its `Map`
-- key with the same predicate; `getRandomType` did not.
-- One seed per half of `DataTypeMap::isValidKeyType`: seed 81 drew a `Nullable` key, seed 146 a
-- `LowCardinality(Nullable)` one.
-- `ast_fuzzer_runs = 0` keeps the server-side fuzzer from mutating the `fuzzQuery` arguments
-- (the Stress test profile sets it to 5), which would change the seed and the length cap.
-- The `Map(` matches below are anchored with `\b` so an aggregate-combinator name cannot satisfy
-- them: the fuzzer appends the `-Map` combinator and draws from a list holding `sumMappedArrays`
-- and friends, and `sumMap(x)` contains the substring `Map(` without any `Map` data type.
-- `match` is RE2, so no lookahead is available; the non-`String` test is `extractAll` plus a
-- comparison.

-- Seed 81 must both survive the generator AND still emit a `Map`: declining to build the `Map`
-- whenever its key draw is invalid would keep a bare `count() > 0` green while removing the
-- fuzzer's `Map` coverage. The assertion names no key type.
SELECT count() > 0 AND countIf(match(query, '\\bMap\\(')) > 0
FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 81) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 146) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;

-- Preservation control: the guard must replace only an INVALID key, so a VALID non-`String` key
-- has to survive. Without this, an unconditional `key_type = String` would keep both rows above
-- green while silently collapsing the fuzzer's whole `Map` key coverage to `String`.
-- The assertion names no type: it only requires that some emitted `Map` key is not `String`, so
-- an unrelated change to the fuzzer's draw consumption can change WHICH valid key appears without
-- reddening the row. Seed 144 is used because neither seed above emits a surviving key (81 emits
-- only the substituted `Map(String, UInt16)`, 146 emits no `Map` at all); it draws
-- `Map(LowCardinality(UInt64), Int32)`, a key that is `LowCardinality` but not
-- `LowCardinality(Nullable)`, which is exactly the boundary `isValidKeyType` draws.
SELECT countIf(arrayExists(x -> x != 'String', extractAll(query, '\\bMap\\(([A-Za-z0-9_]+)'))) > 0
FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 144) LIMIT 200) SETTINGS ast_fuzzer_runs = 0;
