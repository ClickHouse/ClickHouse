-- Regression test: `QueryFuzzer::getRandomType` composed a `Map` whose key was drawn without
-- validation, so `DataTypeMap`'s constructor rejected the fuzzer's own generated type with
-- `BAD_ARGUMENTS` and the whole mutation was aborted. `fuzzDataType` already guarded its `Map`
-- key with the same predicate; `getRandomType` did not.
-- One seed per half of `DataTypeMap::isValidKeyType`: seed 15 draws a `Nullable` key, seed 9189 a
-- `LowCardinality(Nullable)` one.
-- `ast_fuzzer_runs = 0` keeps the server-side fuzzer from mutating the `fuzzQuery` arguments
-- (the Stress test profile sets it to 5), which would change the seed and the length cap.
-- The `Map(` matches below are anchored with `\b` so an aggregate-combinator name cannot satisfy
-- them: the fuzzer appends the `-Map` combinator and draws from a list holding `sumMappedArrays`
-- and friends, and `sumMap(x)` contains the substring `Map(` without any `Map` data type.
-- `match` is RE2, so no lookahead is available; the non-`String` test is `extractAll` plus a
-- comparison.
--
-- The seeds and the row count are tied to the fuzzer's random stream: any change to the number or
-- the order of the `fuzz_rand` draws (adding one option to a fuzzed setting list is enough) moves
-- every downstream decision, so a seed that used to reach the guard stops reaching it. When that
-- happens, re-derive the seeds instead of dropping the assertions: build one binary with the
-- `isValidKeyType` substitution in `getRandomType` removed, then scan seeds for the ones where
-- `fuzzQuery` fails with `Map cannot have a key of type ...`, one seed per half of the predicate.
-- The mutations accumulate complexity row by row, so the guard is reached after a few hundred
-- rows rather than immediately - hence `LIMIT 2000` (about half a second per query).

-- Seed 15 must both survive the generator AND still emit a `Map`: declining to build the `Map`
-- whenever its key draw is invalid would keep a bare `count() > 0` green while removing the
-- fuzzer's `Map` coverage. The assertion names no key type.
SELECT count() > 0 AND countIf(match(query, '\\bMap\\(')) > 0
FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 15) LIMIT 2000) SETTINGS ast_fuzzer_runs = 0;
SELECT count() > 0 FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 9189) LIMIT 2000) SETTINGS ast_fuzzer_runs = 0;

-- Preservation control: the guard must replace only an INVALID key, so a VALID non-`String` key
-- has to survive. Without this, an unconditional `key_type = String` would keep both rows above
-- green while silently collapsing the fuzzer's whole `Map` key coverage to `String`.
-- The assertion names no type: it only requires that some emitted `Map` key is not `String`, so
-- an unrelated change to the fuzzer's draw consumption can change WHICH valid key appears without
-- reddening the row. Seed 90 is used because it emits many surviving non-`String` keys while
-- never reaching the guard itself, so this row keeps its meaning even when the two seeds above
-- have to be re-derived.
SELECT countIf(arrayExists(x -> x != 'String', extractAll(query, '\\bMap\\(([A-Za-z0-9_]+)'))) > 0
FROM (SELECT * FROM fuzzQuery('SELECT 1 FROM numbers(10) WHERE and()', 2000, 90) LIMIT 2000) SETTINGS ast_fuzzer_runs = 0;
