-- Tags: no-fasttest
-- Test optimized JSON-to-JSON type conversion that avoids full serialize+parse
-- when only a subset of paths are affected by parameter changes.
-- All INSERT statements use multi-value VALUES to ensure multi-row blocks.

SET allow_experimental_json_type = 1;
SET json_use_optimized_type_conversion = 1;

SELECT '--- Identity conversion (only metadata changes) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(SKIP a)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 10, "c": "x"}'), ('{"a": 2, "b": 20, "c": "y"}'), ('{"a": 3, "b": 30, "c": "z"}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY data.b::Int64;
DROP TABLE t_json_opt;

SELECT '--- Unchanged typed paths (pointer reuse) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, b String)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "x", "c": 10}'), ('{"a": 2, "b": "y", "c": 20}'), ('{"a": 3, "b": "z", "c": 30}');
-- Only adding typed path c, paths a and b are unchanged.
SELECT data::JSON(a Int32, b String, c Int64) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Changed typed path (CAST Int32 -> Int64) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "x"}'), ('{"a": 2, "b": "y"}'), ('{"a": -100, "b": "z"}');
SELECT (data::JSON(a Int64)).a FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- New typed path from dynamic (canCastDynamicToTyped=true, Int64->Int32) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "x"}'), ('{"a": 2, "b": "y"}'), ('{"a": 3, "b": "z"}');
SELECT (data::JSON(a Int32)).a FROM t_json_opt ORDER BY (data::JSON(a Int32)).a;
DROP TABLE t_json_opt;

SELECT '--- New typed path from dynamic (canCastDynamicToTyped=false, mixed variant types) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
-- Dynamic column for "a" will have both Int64 and String variants.
INSERT INTO t_json_opt VALUES ('{"a": 1}'), ('{"a": "text"}'), ('{"a": 42}');
SELECT data::JSON(a String) as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;

SELECT '--- New typed path from shared data ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 1)) ENGINE = Memory;
-- With max_dynamic_paths=1, excess paths end up in shared data.
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20, "c": 30}'), ('{"a": 100, "b": 200, "c": 300}');
SELECT (data::JSON(a Int32, max_dynamic_paths = 1)).a FROM t_json_opt ORDER BY (data::JSON(a Int32, max_dynamic_paths = 1)).a;
DROP TABLE t_json_opt;

SELECT '--- New typed path from shared data (present in some rows, absent in others) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20}'), ('{"a": 100, "b": 200, "c": 300}');
-- c is in shared data for rows 1,3 but absent in row 2 — should get default.
SELECT (data::JSON(c Int32, max_dynamic_paths = 1)).c FROM t_json_opt ORDER BY data.a::Int64;
DROP TABLE t_json_opt;

SELECT '--- New typed path absent in source (fill with defaults) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1}'), ('{"a": 2}'), ('{"a": 3}');
SELECT (data::JSON(b String)).b FROM t_json_opt;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: promotable, already canonical (Int64) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int64, b String)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 100, "b": "x"}'), ('{"a": 200, "b": "y"}'), ('{"a": 300, "b": "z"}');
SELECT data::JSON(b String) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: promotable, needs CAST (Int8 -> Int64) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int8, b String)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "x"}'), ('{"a": -5, "b": "y"}'), ('{"a": 127, "b": "z"}');
SELECT data::JSON(b String) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: promotable Float32 -> Float64 ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Float32)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1.5}'), ('{"a": 2.5}'), ('{"a": 3.5}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY data.a::Float64;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: promotable Date (with date inference) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Date)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": "2024-01-15"}'), ('{"a": "2024-06-01"}'), ('{"a": "2025-12-31"}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY data.a SETTINGS input_format_try_infer_dates = 1;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: non-promotable (UUID via convertTypedColumnToDynamic) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a UUID)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": "550e8400-e29b-41d4-a716-446655440000"}'), ('{"a": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}'), ('{"a": "00000000-0000-0000-0000-000000000000"}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY data.a::String;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: Nullable with NULLs ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Nullable(Int32))) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1}'), ('{"a": null}'), ('{"a": 3}'), ('{"b": 99}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: overflow to shared data (no dynamic slots) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, b Int32, c Int32, max_dynamic_paths = 0)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20, "c": 30}'), ('{"a": 100, "b": 200, "c": 300}');
-- Remove a, b, c typed paths. max_dynamic_paths=0 means they must go to shared data.
SELECT data::JSON(max_dynamic_paths = 0) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Removed typed path: some fit in dynamic, rest overflow to shared data ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, b Int32, c Int32, max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20, "c": 30}');
-- Remove all 3 typed paths with only 1 dynamic slot: first goes to dynamic, rest to shared data.
SELECT data::JSON(max_dynamic_paths = 1) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Skip: dynamic path skipped ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20, "c": 30}');
SELECT data::JSON(SKIP b) as converted FROM t_json_opt ORDER BY data.a::Int64;
DROP TABLE t_json_opt;

SELECT '--- Skip: sub-path matching ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": {"x": 1, "y": 2}, "b": 3}'), ('{"a": {"x": 10, "y": 20}, "b": 30}');
-- SKIP a should remove a.x and a.y (sub-paths of a).
SELECT data::JSON(SKIP a) as converted FROM t_json_opt ORDER BY data.b::Int64;
DROP TABLE t_json_opt;

SELECT '--- Skip: regexp matching ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b_secret": 2, "c": 3, "d_secret_key": 4}'), ('{"a": 10, "b_secret": 20, "c": 30, "d_secret_key": 40}');
SELECT data::JSON(SKIP REGEXP '^b') as converted FROM t_json_opt ORDER BY data.a::Int64;
DROP TABLE t_json_opt;

SELECT '--- Skip: shared data entries skipped ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20, "c": 30}');
-- Some paths are in shared data; skip rule should remove them.
SELECT data::JSON(SKIP c, max_dynamic_paths = 1) as converted FROM t_json_opt ORDER BY data.a::Int64;
DROP TABLE t_json_opt;

SELECT '--- Skip: removed typed path matches skip rule (exact) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, b String)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "x", "c": 10}'), ('{"a": 2, "b": "y", "c": 20}'), ('{"a": 3, "b": "z", "c": 30}');
-- Remove typed path b AND skip it: b should be absent from the result entirely.
SELECT data::JSON(a Int32, SKIP b) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Skip: removed typed path matches skip rule (regexp) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, secret_key String)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "secret_key": "abc", "c": 10}'), ('{"a": 2, "secret_key": "def", "c": 20}'), ('{"a": 3, "secret_key": "ghi", "c": 30}');
-- Remove typed path secret_key AND skip it via regexp.
SELECT data::JSON(a Int32, SKIP REGEXP '^secret') as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Skip: removed typed path matches skip rule (sub-path) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a.x Int32, a.y Int32)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": {"x": 1, "y": 2}, "b": 3}'), ('{"a": {"x": 10, "y": 20}, "b": 30}'), ('{"a": {"x": 100, "y": 200}, "b": 300}');
-- Remove typed paths a.x and a.y, skip parent path a: both should be absent.
SELECT data::JSON(SKIP a) as converted FROM t_json_opt ORDER BY data.b::Int64;
DROP TABLE t_json_opt;

SELECT '--- Shared data reusable (no changes affect it) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3}'), ('{"a": 10, "b": 20, "c": 30}');
-- Adding typed path from dynamic — shared data untouched.
SELECT data::JSON(a Int32, max_dynamic_paths = 1) as converted FROM t_json_opt ORDER BY (data::JSON(a Int32, max_dynamic_paths = 1)).a;
DROP TABLE t_json_opt;

SELECT '--- Shared data NOT reusable: new typed path found in shared data ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 0)) ENGINE = Memory;
-- All paths go to shared data with max_dynamic_paths=0.
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2}'), ('{"a": 10, "b": 20}'), ('{"a": 100}');
-- Promoting "a" from shared data to typed path: row 3 has no "b" in shared data.
SELECT data::JSON(a Int32, max_dynamic_paths = 0) as converted FROM t_json_opt ORDER BY (data::JSON(a Int32, max_dynamic_paths = 0)).a;
DROP TABLE t_json_opt;

SELECT '--- Phase 7: merge removed typed paths into shared data (sorted merge) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, d Int32, max_dynamic_paths = 0)) ENGINE = Memory;
-- Existing shared data has paths b, c. Removed typed paths are a, d. Merge must maintain sorted order.
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3, "d": 4}'), ('{"a": 10, "b": 20, "c": 30, "d": 40}');
SELECT data::JSON(max_dynamic_paths = 0) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- Combination: add typed + remove typed + skip ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "hello", "c": 42, "d": "keep"}'), ('{"a": 2, "b": "world", "c": 43, "d": "this"}'), ('{"a": 3, "b": "foo", "c": 44, "d": "too"}');
-- Remove a, add b as typed, skip c.
SELECT data::JSON(b String, SKIP c) as converted FROM t_json_opt ORDER BY (data::JSON(b String, SKIP c)).b;
DROP TABLE t_json_opt;

SELECT '--- Structural type: adding Tuple typed path triggers format+parse fallback ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": [1, 2]}'), ('{"a": [3, 4]}'), ('{"a": [5, 6]}');
SELECT data::JSON(a Tuple(Int64, Int64)) as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;

SELECT '--- Structural type: removing Map typed path triggers fallback ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Map(String, Int64))) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": {"k1": 1}}'), ('{"a": {"k2": 2}}'), ('{"a": {"k3": 3}}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;

SELECT '--- dynamic_limits_changed: fallback to format+parse ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 10)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2}'), ('{"a": 10, "b": 20}');
SELECT data::JSON(max_dynamic_paths = 5) as converted FROM t_json_opt ORDER BY data.a::Int64;
DROP TABLE t_json_opt;

SELECT '--- Setting disabled: force format+parse ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": "hello"}'), ('{"a": 2, "b": "world"}');
SELECT data::JSON(a Int64) as converted FROM t_json_opt ORDER BY data.a
SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t_json_opt;

SELECT '--- Empty column ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32)) ENGINE = Memory;
SELECT data::JSON(a Int64) as converted FROM t_json_opt;
DROP TABLE t_json_opt;

SELECT '--- Verify optimized matches format+parse for complex scenario ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, b Float32)) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 1.5, "c": "hello", "d": [1,2,3]}'), ('{"a": 2, "b": 2.5, "c": "world", "d": [4,5]}'), ('{"a": 3, "b": 3.5, "e": true}');

SELECT 'optimized:';
SELECT data::JSON(a Int64, c String) as converted FROM t_json_opt ORDER BY data.a
SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'format_parse:';
SELECT data::JSON(a Int64, c String) as converted FROM t_json_opt ORDER BY data.a
SETTINGS json_use_optimized_type_conversion = 0;
DROP TABLE t_json_opt;

SELECT '--- Promotable types: Decimal -> Float64 ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Decimal64(3))) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1.123}'), ('{"a": 2.456}'), ('{"a": 3.789}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY data.a::Float64;
DROP TABLE t_json_opt;

SELECT '--- Promotable types: Array(Int8) -> Array(Int64) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Array(Int8))) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": [1, 2, 3]}'), ('{"a": [4, 5]}'), ('{"a": []}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;

SELECT '--- Promotable types: Nullable(Int16) -> Nullable(Int64) ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Nullable(Int16))) ENGINE = Memory;
INSERT INTO t_json_opt VALUES ('{"a": 1}'), ('{"a": null}'), ('{"a": 100}');
SELECT data::JSON as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;

SELECT '--- Max dynamic paths locked when shared data non-empty ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(a Int32, b Int32, max_dynamic_paths = 2)) ENGINE = Memory;
-- With max_dynamic_paths=2 and typed paths a, b, plus dynamic paths c, d, path e goes to shared data.
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}'), ('{"a": 10, "b": 20, "c": 30, "d": 40, "e": 50}');
-- Remove typed paths a, b. They should become dynamic. e stays in shared data.
SELECT data::JSON(max_dynamic_paths = 2) as converted FROM t_json_opt ORDER BY data.a;
DROP TABLE t_json_opt;

SELECT '--- sharedDataIsAffectedByNewTypedPaths with mixed rows ---';
DROP TABLE IF EXISTS t_json_opt;
CREATE TABLE t_json_opt (data JSON(max_dynamic_paths = 0)) ENGINE = Memory;
-- Row 1: shared data has a, b. Row 2: shared data has a only. Row 3: shared data has b, c.
INSERT INTO t_json_opt VALUES ('{"a": 1, "b": 2}'), ('{"a": 10}'), ('{"b": 20, "c": 30}');
-- Adding typed path "b": present in rows 1 and 3 shared data, absent in row 2.
SELECT data::JSON(b Int32, max_dynamic_paths = 0) as converted FROM t_json_opt ORDER BY toString(data.a);
DROP TABLE t_json_opt;
