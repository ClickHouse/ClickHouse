-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101937
--
-- `CoalescingMergeTree` (which delegates to `SummingSortedAlgorithm` with
-- `aggregate_all_columns = true`) used to silently drop nested `Map` columns
-- from both the `columns_to_aggregate` and `column_numbers_not_to_aggregate`
-- lists in `defineColumns`. The unset slot in `res_columns` then surfaced as a
-- null `ColumnPtr` inside `postprocessChunk`, which `Chunk::checkNumRowsIsConsistent`
-- dereferenced and crashed the server (SIGSEGV in release builds, `px != 0`
-- assertion in debug builds, UBSan `member call on null pointer` in arm_asan_ubsan).
--
-- A nested table whose name ends with `Map` triggers the bug (`testMap.key`,
-- `legacy_features_Map.id`, etc.) — the original AST fuzzer reproducer used
-- `legacy_features_Map.id Array(LowCardinality(IPv6))` together with a non-Array
-- sibling.

DROP TABLE IF EXISTS test_coal_nested_map_array;
DROP TABLE IF EXISTS test_coal_nested_map_mixed;
DROP TABLE IF EXISTS test_coal_nested_map_lc;

-- 1. Classic two-Array nested-Map convention: this is the original reproducer.

CREATE TABLE test_coal_nested_map_array
(
    key UInt32,
    `testMap.key` Array(String),
    `testMap.value` Array(UInt32)
)
ENGINE = CoalescingMergeTree
ORDER BY key;

INSERT INTO test_coal_nested_map_array VALUES (1, ['a'], [10]);
INSERT INTO test_coal_nested_map_array VALUES (1, ['b'], [20]);

SELECT 'array - before final', * FROM test_coal_nested_map_array ORDER BY key, `testMap.key`;
SELECT 'array - FINAL', * FROM test_coal_nested_map_array FINAL ORDER BY key;
OPTIMIZE TABLE test_coal_nested_map_array FINAL;
SELECT 'array - after optimize', * FROM test_coal_nested_map_array ORDER BY key;

-- 2. Mixed shape: an Array Map-suffixed column next to a non-Array sibling — this
--    is the exact shape that fuzzed PR #104563 and PR #104281 onto STID 1003-326e
--    on `arm_asan_ubsan`.

CREATE TABLE test_coal_nested_map_mixed
(
    key UInt128,
    `legacy_features_Map.id` Array(IPv6),
    `legacy_features_Map.count` Nullable(Int32)
)
ENGINE = CoalescingMergeTree
ORDER BY key;

INSERT INTO test_coal_nested_map_mixed (key) VALUES (1);
INSERT INTO test_coal_nested_map_mixed VALUES (1, ['::1'], 42);
SELECT 'mixed - FINAL', * FROM test_coal_nested_map_mixed FINAL;
OPTIMIZE TABLE test_coal_nested_map_mixed FINAL;
SELECT 'mixed - after optimize', * FROM test_coal_nested_map_mixed;

-- 3. Same as (1) but with `LowCardinality` inside the nested Array — the exact
--    type used by the fuzzer reproducer (`Array(LowCardinality(IPv6))`).

CREATE TABLE test_coal_nested_map_lc
(
    key UInt32,
    `myMap.id` Array(LowCardinality(String)),
    `myMap.value` Array(UInt64)
)
ENGINE = CoalescingMergeTree
ORDER BY key;

INSERT INTO test_coal_nested_map_lc VALUES (1, ['x'], [100]);
INSERT INTO test_coal_nested_map_lc VALUES (1, ['y'], [200]);
SELECT 'lc - FINAL', * FROM test_coal_nested_map_lc FINAL ORDER BY key;
OPTIMIZE TABLE test_coal_nested_map_lc FINAL;
SELECT 'lc - after optimize', * FROM test_coal_nested_map_lc;

DROP TABLE test_coal_nested_map_array;
DROP TABLE test_coal_nested_map_mixed;
DROP TABLE test_coal_nested_map_lc;
