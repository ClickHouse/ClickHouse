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
DROP TABLE IF EXISTS t_narrow_array;
