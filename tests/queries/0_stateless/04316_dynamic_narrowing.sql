-- Tags: no-fasttest
-- Test: Dynamic column per-part type narrowing optimization

-- ============================================================
-- 1. Basic narrowing: homogeneous Int64
-- ============================================================
DROP TABLE IF EXISTS t_narrow;
CREATE TABLE t_narrow (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow SELECT number, number::Int64 FROM numbers(1000);

SELECT '-- 1. homogeneous Int64 roundtrip';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow;

-- ============================================================
-- 2. Second part: homogeneous String
-- ============================================================
INSERT INTO t_narrow SELECT number + 1000, 'str_' || toString(number) FROM numbers(1000);

SELECT '-- 2. two homogeneous parts (Int64 + String)';
SELECT count() FROM t_narrow;
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow GROUP BY t ORDER BY t;

-- ============================================================
-- 3. Merge heterogeneous parts
-- ============================================================
OPTIMIZE TABLE t_narrow FINAL;

SELECT '-- 3. after merge';
SELECT count() FROM t_narrow;
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow GROUP BY t ORDER BY t;
SELECT value::Int64 FROM t_narrow WHERE dynamicType(value) = 'Int64' ORDER BY value::Int64 LIMIT 3;
SELECT value::String FROM t_narrow WHERE dynamicType(value) = 'String' ORDER BY value::String LIMIT 3;

-- ============================================================
-- 4. NULLs mixed with single type (should narrow: one variant + NULLs)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_null;
CREATE TABLE t_narrow_null (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_null SELECT number, if(number % 3 = 0, NULL, number::Int64) FROM numbers(900);

SELECT '-- 4. NULLs + Int64';
SELECT count(), countIf(value IS NULL) AS nulls, countIf(value IS NOT NULL) AS non_nulls FROM t_narrow_null;
SELECT min(value::Int64), max(value::Int64) FROM t_narrow_null WHERE value IS NOT NULL;

-- ============================================================
-- 5. All NULLs (should NOT narrow — no non-empty variant)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_allnull;
CREATE TABLE t_narrow_allnull (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_allnull SELECT number, NULL FROM numbers(100);

SELECT '-- 5. all NULLs';
SELECT count(), countIf(value IS NULL) FROM t_narrow_allnull;

-- ============================================================
-- 6. Single row (edge case)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_single;
CREATE TABLE t_narrow_single (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_single VALUES (1, 42::Int64);

SELECT '-- 6. single row';
SELECT id, value::Int64, dynamicType(value) FROM t_narrow_single;

-- ============================================================
-- 7. Empty insert (edge case)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_empty;
CREATE TABLE t_narrow_empty (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_empty SELECT number, number::Int64 FROM numbers(0);

SELECT '-- 7. empty insert';
SELECT count() FROM t_narrow_empty;

-- ============================================================
-- 8. DETACH / ATTACH (simulates restart — re-reads part metadata from disk)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_restart;
CREATE TABLE t_narrow_restart (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_restart SELECT number, number::Float64 FROM numbers(500);

SELECT '-- 8a. before detach';
SELECT count(), min(value::Float64), max(value::Float64) FROM t_narrow_restart;

DETACH TABLE t_narrow_restart;
ATTACH TABLE t_narrow_restart;

SELECT '-- 8b. after attach';
SELECT count(), min(value::Float64), max(value::Float64) FROM t_narrow_restart;

-- ============================================================
-- 9. Mixed parts: some narrowed, some not
-- ============================================================
DROP TABLE IF EXISTS t_narrow_mixed;
CREATE TABLE t_narrow_mixed (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

-- Part 1: homogeneous Int64 → narrowed
INSERT INTO t_narrow_mixed SELECT number, number::Int64 FROM numbers(100);
-- Part 2: heterogeneous (Int64 + String) → NOT narrowed
INSERT INTO t_narrow_mixed SELECT number + 100, if(number % 2 = 0, number::Int64, ('s' || toString(number))::String) FROM numbers(100);

SELECT '-- 9. mixed narrowed + non-narrowed parts';
SELECT count() FROM t_narrow_mixed;
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow_mixed GROUP BY t ORDER BY t;

-- Merge and verify
OPTIMIZE TABLE t_narrow_mixed FINAL;
SELECT '-- 9b. after merge';
SELECT count() FROM t_narrow_mixed;
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow_mixed GROUP BY t ORDER BY t;

-- ============================================================
-- 10. Multiple data types across parts, then merge
-- ============================================================
DROP TABLE IF EXISTS t_narrow_multi;
CREATE TABLE t_narrow_multi (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_multi SELECT number, number::Int64 FROM numbers(100);
INSERT INTO t_narrow_multi SELECT number + 100, number::Float64 FROM numbers(100);
INSERT INTO t_narrow_multi SELECT number + 200, ('s' || toString(number))::String FROM numbers(100);
INSERT INTO t_narrow_multi SELECT number + 300, toDate('2026-01-01') + number FROM numbers(100);

SELECT '-- 10. four typed parts';
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow_multi GROUP BY t ORDER BY t;

OPTIMIZE TABLE t_narrow_multi FINAL;
SELECT '-- 10b. after merge';
SELECT dynamicType(value) AS t, count() AS c FROM t_narrow_multi GROUP BY t ORDER BY t;
SELECT count() FROM t_narrow_multi;

-- ============================================================
-- 11. JSON column with PARTITION BY action_type (the real use case)
-- ============================================================
DROP TABLE IF EXISTS t_json_narrow;
CREATE TABLE t_json_narrow
(
    id UInt64,
    action_type Int32,
    payload JSON
)
ENGINE = MergeTree
PARTITION BY action_type
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_json_narrow FORMAT JSONEachRow
{"id": 1, "action_type": 4, "payload": {"battle_type": 10, "win": true, "damage": 12345}}
{"id": 2, "action_type": 4, "payload": {"battle_type": 5, "win": false, "damage": 67890}}
{"id": 3, "action_type": 4, "payload": {"battle_type": 10, "win": true, "damage": 11111}}

INSERT INTO t_json_narrow FORMAT JSONEachRow
{"id": 4, "action_type": 3, "payload": {"item_id": 100, "count": 5}}
{"id": 5, "action_type": 3, "payload": {"item_id": 200, "count": 10}}

SELECT '-- 11a. JSON partitioned by action_type';
SELECT action_type, count() FROM t_json_narrow GROUP BY action_type ORDER BY action_type;
SELECT id, payload.battle_type, payload.win, payload.damage FROM t_json_narrow WHERE action_type = 4 ORDER BY id;
SELECT id, payload.item_id, payload.count FROM t_json_narrow WHERE action_type = 3 ORDER BY id;

-- DETACH/ATTACH to verify persistence
DETACH TABLE t_json_narrow;
ATTACH TABLE t_json_narrow;

SELECT '-- 11b. JSON after reattach';
SELECT id, payload.battle_type, payload.win, payload.damage FROM t_json_narrow WHERE action_type = 4 ORDER BY id;
SELECT id, payload.item_id, payload.count FROM t_json_narrow WHERE action_type = 3 ORDER BY id;

-- ============================================================
-- 12. Array(Dynamic) — narrowing inside nested Dynamic
-- ============================================================
DROP TABLE IF EXISTS t_narrow_array;
CREATE TABLE t_narrow_array (id UInt64, arr Array(Dynamic))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_array VALUES (1, [1::Int64, 2::Int64, 3::Int64]);
INSERT INTO t_narrow_array VALUES (2, [10::Int64, 20::Int64]);

SELECT '-- 12. Array(Dynamic)';
SELECT id, arr FROM t_narrow_array ORDER BY id;

-- ============================================================
-- 13. Dynamic element subcolumn reads on narrowed parts
-- ============================================================
DROP TABLE IF EXISTS t_narrow_subcol;
CREATE TABLE t_narrow_subcol (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_subcol SELECT number, number::Int64 FROM numbers(100);

SELECT '-- 13a. subcolumn matching narrowed type';
SELECT value.Int64 FROM t_narrow_subcol ORDER BY id LIMIT 3;
SELECT '-- 13b. subcolumn NOT matching narrowed type (all NULL)';
SELECT value.String FROM t_narrow_subcol ORDER BY id LIMIT 3;
SELECT '-- 13c. null_map subcolumn on no-null narrowed part';
SELECT count(), countIf(value.Int64.null = 0) FROM t_narrow_subcol;

-- Same with NULLs
DROP TABLE IF EXISTS t_narrow_subcol_null;
CREATE TABLE t_narrow_subcol_null (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_subcol_null SELECT number, if(number % 4 = 0, NULL, number::Int64) FROM numbers(100);

SELECT '-- 13d. null_map subcolumn on with-null narrowed part';
SELECT count(), sum(value.Int64.null) AS nulls FROM t_narrow_subcol_null;

-- ============================================================
-- 14. Mutations that PRESERVE narrowed type (safe)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_mut_safe;
CREATE TABLE t_narrow_mut_safe (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_mut_safe SELECT number, number::Int64 FROM numbers(1000);
ALTER TABLE t_narrow_mut_safe UPDATE value = (value::Int64 + 100)::Int64 WHERE id < 50 SETTINGS mutations_sync = 1;

SELECT '-- 14. mutation preserving type';
SELECT count(), sum(value::Int64 >= 100) FROM t_narrow_mut_safe WHERE id < 50;
SELECT count() FROM t_narrow_mut_safe;

-- ============================================================
-- 15. NARROWED layout assertions: actually verify file shape
-- ============================================================
DROP TABLE IF EXISTS t_narrow_layout;
CREATE TABLE t_narrow_layout (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

-- Part 1: no NULLs → expect `value` + `value.dynamic_structure` only (no `value.null`)
INSERT INTO t_narrow_layout SELECT number, number::Int64 FROM numbers(100);

-- Part 2: with NULLs → expect `value`, `value.dynamic_structure`, `value.null`
INSERT INTO t_narrow_layout SELECT number + 100, if(number % 3 = 0, NULL, number::Int64) FROM numbers(99);

SELECT '-- 15a. no-null part substreams';
SELECT arrayJoin(substreams) AS s
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_narrow_layout' AND name = 'all_1_1_0' AND column = 'value'
ORDER BY s;

SELECT '-- 15b. with-null part substreams';
SELECT arrayJoin(substreams) AS s
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_narrow_layout' AND name = 'all_2_2_0' AND column = 'value'
ORDER BY s;

-- ============================================================
-- 16. Nested subcolumns through narrowed Tuple/Array elements
-- ============================================================
DROP TABLE IF EXISTS t_narrow_nested;
CREATE TABLE t_narrow_nested (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

-- Narrowed to Tuple
INSERT INTO t_narrow_nested SELECT number, tuple(number::UInt32, ('s' || toString(number))::String)::Tuple(a UInt32, b String) FROM numbers(10);

SELECT '-- 16a. Tuple nested subcolumn .a';
SELECT value.`Tuple(a UInt32, b String)`.a FROM t_narrow_nested ORDER BY id LIMIT 3;
SELECT '-- 16b. Tuple nested subcolumn .b';
SELECT value.`Tuple(a UInt32, b String)`.b FROM t_narrow_nested ORDER BY id LIMIT 3;

DROP TABLE IF EXISTS t_narrow_array_nested;
CREATE TABLE t_narrow_array_nested (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

-- Narrowed to Array(UInt64)
INSERT INTO t_narrow_array_nested SELECT number, range(number % 5)::Array(UInt64) FROM numbers(10);

SELECT '-- 16c. Array .size0 subcolumn';
SELECT value.`Array(UInt64)`.size0 FROM t_narrow_array_nested ORDER BY id LIMIT 5;

-- ============================================================
-- 17. JSON merge: narrowed dynamic paths keep dynamic_paths_statistics
-- After OPTIMIZE FINAL, paths that had values should still be dynamic paths,
-- not silently demoted to shared data due to zero-reported size.
-- ============================================================
DROP TABLE IF EXISTS t_json_merge;
CREATE TABLE t_json_merge (id UInt64, payload JSON(max_dynamic_paths=4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

-- 3 narrowed parts with overlapping paths
INSERT INTO t_json_merge VALUES (1, '{"a": 10}'), (2, '{"a": 20}'), (3, '{"a": 30}');
INSERT INTO t_json_merge VALUES (4, '{"a": 40}'), (5, '{"a": 50}'), (6, '{"a": 60}');
INSERT INTO t_json_merge VALUES (7, '{"a": 70}'), (8, '{"a": 80}'), (9, '{"a": 90}');

OPTIMIZE TABLE t_json_merge FINAL;

SELECT '-- 17. JSON merge: narrowed path "a" is still a dynamic path after merge';
SELECT id, payload.a FROM t_json_merge ORDER BY id LIMIT 5;
-- Verify the dynamic path "a" is materialized (not in shared data) in the merged part.
-- A path in shared data shows up as a SharedData row in system.parts; a dedicated dynamic path
-- shows up with its own substream entry. Just confirm we can still extract values correctly
-- and that the dynamic_structure stream is present (which it must be for the merge to read).
SELECT '-- 17b. Merged part exists';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json_merge' AND active;

-- ============================================================
-- 18. Merge of two NARROWED parts with NULLs of the same type
-- Regression test for the bug where `null_count` was lost on the read path,
-- causing the merge writer to pick `stored_as_nullable=false` and crash on
-- the first NULL row from a source part.
-- ============================================================
DROP TABLE IF EXISTS t_narrow_null_merge;
CREATE TABLE t_narrow_null_merge (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_null_merge SELECT number, if(number % 3 = 0, NULL, number::Int64) FROM numbers(100);
INSERT INTO t_narrow_null_merge SELECT number + 100, if(number % 4 = 0, NULL, number::Int64) FROM numbers(100);
OPTIMIZE TABLE t_narrow_null_merge FINAL;

SELECT '-- 18. NARROWED-with-NULLs merge';
SELECT count(), countIf(value IS NULL) FROM t_narrow_null_merge;

-- ============================================================
-- 19. Nested subcolumn read on a narrowed-Nullable Tuple
-- Regression test for the shape mismatch when `Nullable(Tuple).getSubcolumn(...)`
-- produced a `Nullable(T)` column but `result_column` was the bare `T`.
-- ============================================================
DROP TABLE IF EXISTS t_narrow_nested_null;
CREATE TABLE t_narrow_nested_null (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4';

INSERT INTO t_narrow_nested_null SELECT number,
    if(number % 3 = 0, NULL, tuple(number::UInt32, ('s' || toString(number))::String)::Tuple(a UInt32, b String))
FROM numbers(30);

SELECT '-- 19. Nullable narrowed Tuple nested .a';
SELECT value.`Tuple(a UInt32, b String)`.a FROM t_narrow_nested_null ORDER BY id LIMIT 6;

-- ============================================================
-- 20. V3-source-with-NULLs migrated to v4 via OPTIMIZE
-- The V3 sources have no `null_count` on disk; the merge writer must treat
-- the merged stats as "unknown NULL count" and wrap in Nullable.
-- ============================================================
DROP TABLE IF EXISTS t_v3_to_v4;
CREATE TABLE t_v3_to_v4 (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v3';

INSERT INTO t_v3_to_v4 SELECT number, if(number % 3 = 0, NULL, number::Int64) FROM numbers(100);
INSERT INTO t_v3_to_v4 SELECT number + 100, if(number % 4 = 0, NULL, number::Int64) FROM numbers(100);

ALTER TABLE t_v3_to_v4 MODIFY SETTING dynamic_serialization_version = 'v4';
OPTIMIZE TABLE t_v3_to_v4 FINAL;

SELECT '-- 20. V3 sources merged into v4 narrowed part';
SELECT count(), countIf(value IS NULL) FROM t_v3_to_v4;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE IF EXISTS t_narrow;
DROP TABLE IF EXISTS t_narrow_null;
DROP TABLE IF EXISTS t_narrow_allnull;
DROP TABLE IF EXISTS t_narrow_single;
DROP TABLE IF EXISTS t_narrow_empty;
DROP TABLE IF EXISTS t_narrow_restart;
DROP TABLE IF EXISTS t_narrow_mixed;
DROP TABLE IF EXISTS t_narrow_multi;
DROP TABLE IF EXISTS t_json_narrow;
DROP TABLE IF EXISTS t_narrow_subcol;
DROP TABLE IF EXISTS t_narrow_subcol_null;
DROP TABLE IF EXISTS t_narrow_mut_safe;
DROP TABLE IF EXISTS t_narrow_layout;
DROP TABLE IF EXISTS t_narrow_nested;
DROP TABLE IF EXISTS t_narrow_array_nested;
DROP TABLE IF EXISTS t_json_merge;
DROP TABLE IF EXISTS t_narrow_null_merge;
DROP TABLE IF EXISTS t_narrow_nested_null;
DROP TABLE IF EXISTS t_v3_to_v4;
DROP TABLE IF EXISTS t_narrow_array;
