-- Tags: no-fasttest
-- Test: Dynamic column narrowing — extended test coverage
-- Covers: compact parts, large data (multi-granule), nested JSON, multi-column,
--         INSERT SELECT, ALTER, mutation, SharedVariant overflow

-- ============================================================
-- 1. Compact part (default small part format)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_compact;
CREATE TABLE t_narrow_compact (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id;
-- Default settings: small inserts produce Compact parts

INSERT INTO t_narrow_compact SELECT number, number::Int64 FROM numbers(100);

SELECT '-- 1. compact part roundtrip';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow_compact;

-- Verify it's actually a Compact part
SELECT name, part_type, rows FROM system.parts
WHERE table = 't_narrow_compact' AND database = currentDatabase() AND active;

DROP TABLE t_narrow_compact;

-- ============================================================
-- 2. Large data (multi-granule, >8192 rows)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_large;
CREATE TABLE t_narrow_large (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 256;
-- Small granularity to force many granules

INSERT INTO t_narrow_large SELECT number, number::Int64 FROM numbers(50000);

SELECT '-- 2. large data multi-granule';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow_large;

-- Verify with a mid-range query (crossing granule boundaries)
SELECT count() FROM t_narrow_large WHERE value::Int64 BETWEEN 10000 AND 20000;

DROP TABLE t_narrow_large;

-- ============================================================
-- 3. Nested JSON: Array(JSON) with inner Dynamic paths
-- ============================================================
DROP TABLE IF EXISTS t_narrow_nested_json;
CREATE TABLE t_narrow_nested_json
(
    id UInt64,
    data JSON
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_narrow_nested_json FORMAT JSONEachRow
{"id": 1, "data": {"items": [{"name": "sword", "level": 10}, {"name": "shield", "level": 5}]}}
{"id": 2, "data": {"items": [{"name": "bow", "level": 8}]}}
{"id": 3, "data": {"items": [{"name": "staff", "level": 12}, {"name": "robe", "level": 3}, {"name": "hat", "level": 7}]}}

SELECT '-- 3. nested JSON Array(JSON)';
SELECT id, data.items FROM t_narrow_nested_json ORDER BY id;

-- DETACH/ATTACH to verify persistence of nested narrowed paths
DETACH TABLE t_narrow_nested_json;
ATTACH TABLE t_narrow_nested_json;

SELECT '-- 3b. after reattach';
SELECT id, data.items FROM t_narrow_nested_json ORDER BY id;

DROP TABLE t_narrow_nested_json;

-- ============================================================
-- 4. Multiple Dynamic columns in one table
-- ============================================================
DROP TABLE IF EXISTS t_narrow_multi_col;
CREATE TABLE t_narrow_multi_col
(
    id UInt64,
    col_a Dynamic,
    col_b Dynamic,
    col_c Dynamic
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Each column gets a different homogeneous type
INSERT INTO t_narrow_multi_col
SELECT number, number::Int64, 'str_' || toString(number), number::Float64
FROM numbers(500);

SELECT '-- 4. multi dynamic columns';
SELECT count(), min(col_a::Int64), max(col_b::String), min(col_c::Float64) FROM t_narrow_multi_col;

DETACH TABLE t_narrow_multi_col;
ATTACH TABLE t_narrow_multi_col;

SELECT '-- 4b. after reattach';
SELECT count(), min(col_a::Int64), max(col_b::String), min(col_c::Float64) FROM t_narrow_multi_col;

DROP TABLE t_narrow_multi_col;

-- ============================================================
-- 5. INSERT SELECT (cross-table copy)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_src;
DROP TABLE IF EXISTS t_narrow_dst;

CREATE TABLE t_narrow_src (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_narrow_dst (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_narrow_src SELECT number, number::Int64 FROM numbers(1000);

INSERT INTO t_narrow_dst SELECT * FROM t_narrow_src;

SELECT '-- 5. INSERT SELECT';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow_dst;

DROP TABLE t_narrow_src;
DROP TABLE t_narrow_dst;

-- ============================================================
-- 6. ALTER ADD/DROP COLUMN with existing narrowed parts
-- ============================================================
DROP TABLE IF EXISTS t_narrow_alter;
CREATE TABLE t_narrow_alter (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_narrow_alter SELECT number, number::Int64 FROM numbers(500);

-- Add a new column — existing narrowed parts should still be readable
ALTER TABLE t_narrow_alter ADD COLUMN extra String DEFAULT 'default_val';

SELECT '-- 6a. after ADD COLUMN';
SELECT count(), min(value::Int64), max(value::Int64), any(extra) FROM t_narrow_alter;

-- Insert new data with the extra column
INSERT INTO t_narrow_alter SELECT number + 500, number::Int64, 'custom' FROM numbers(500);

SELECT '-- 6b. mixed old + new parts';
SELECT count(), uniq(extra) FROM t_narrow_alter;

-- Drop the Dynamic column
ALTER TABLE t_narrow_alter DROP COLUMN value;

SELECT '-- 6c. after DROP Dynamic column';
SELECT count(), any(extra) FROM t_narrow_alter;

DROP TABLE t_narrow_alter;

-- ============================================================
-- 7. Mutation (DELETE / UPDATE)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_mutation;
CREATE TABLE t_narrow_mutation (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_narrow_mutation SELECT number, number::Int64 FROM numbers(1000);

SELECT '-- 7a. before mutation';
SELECT count() FROM t_narrow_mutation;

-- DELETE mutation: removes half the rows, rewrites the part
ALTER TABLE t_narrow_mutation DELETE WHERE id >= 500 SETTINGS mutations_sync = 1;

SELECT '-- 7b. after DELETE';
SELECT count(), min(value::Int64), max(value::Int64) FROM t_narrow_mutation;

-- UPDATE mutation on the Dynamic column
ALTER TABLE t_narrow_mutation UPDATE value = (value::Int64 * 2)::Int64 WHERE id < 100 SETTINGS mutations_sync = 1;

SELECT '-- 7c. after UPDATE';
SELECT count() FROM t_narrow_mutation WHERE value::Int64 >= 200;
SELECT min(value::Int64), max(value::Int64) FROM t_narrow_mutation WHERE id < 100;

DROP TABLE t_narrow_mutation;

-- ============================================================
-- 8. SharedVariant overflow (>max_dynamic_types variants → not narrowable)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_shared;
CREATE TABLE t_narrow_shared (id UInt64, value Dynamic(max_types=2))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Insert 3 different types: Int64, String, Float64 — exceeds max_dynamic_types=2
-- The third type will go to SharedVariant
INSERT INTO t_narrow_shared VALUES (1, 42::Int64), (2, 'hello'), (3, 3.14::Float64);

SELECT '-- 8. SharedVariant overflow (should NOT narrow)';
SELECT id, value, dynamicType(value) FROM t_narrow_shared ORDER BY id;

-- Verify data survives DETACH/ATTACH
DETACH TABLE t_narrow_shared;
ATTACH TABLE t_narrow_shared;

SELECT '-- 8b. after reattach';
SELECT id, value, dynamicType(value) FROM t_narrow_shared ORDER BY id;

DROP TABLE t_narrow_shared;

-- ============================================================
-- 9. Bool type narrowing
-- ============================================================
DROP TABLE IF EXISTS t_narrow_bool;
CREATE TABLE t_narrow_bool (id UInt64, flag Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_narrow_bool SELECT number, (number % 2 = 0)::Bool FROM numbers(1000);

SELECT '-- 9. Bool narrowing';
SELECT count(), countIf(flag::Bool) FROM t_narrow_bool;

DETACH TABLE t_narrow_bool;
ATTACH TABLE t_narrow_bool;

SELECT '-- 9b. after reattach';
SELECT count(), countIf(flag::Bool) FROM t_narrow_bool;

DROP TABLE t_narrow_bool;

-- ============================================================
-- 10. Date/DateTime narrowing
-- ============================================================
DROP TABLE IF EXISTS t_narrow_date;
CREATE TABLE t_narrow_date (id UInt64, ts Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_narrow_date SELECT number, toDateTime('2026-01-01 00:00:00') + number FROM numbers(1000);

SELECT '-- 10. DateTime narrowing';
SELECT count(), min(ts::DateTime), max(ts::DateTime) FROM t_narrow_date;

DROP TABLE t_narrow_date;

-- ============================================================
-- 11. Large NULLs ratio (verify Nullable narrowing at scale)
-- ============================================================
DROP TABLE IF EXISTS t_narrow_sparse_null;
CREATE TABLE t_narrow_sparse_null (id UInt64, value Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- 90% NULLs, 10% Int64
INSERT INTO t_narrow_sparse_null
SELECT number, if(number % 10 = 0, number::Int64, NULL) FROM numbers(10000);

SELECT '-- 11. 90% NULLs';
SELECT count(), countIf(value IS NULL) AS nulls, countIf(value IS NOT NULL) AS non_nulls FROM t_narrow_sparse_null;
SELECT min(value::Int64), max(value::Int64) FROM t_narrow_sparse_null WHERE value IS NOT NULL;

DETACH TABLE t_narrow_sparse_null;
ATTACH TABLE t_narrow_sparse_null;

SELECT '-- 11b. after reattach';
SELECT count(), countIf(value IS NULL) AS nulls, countIf(value IS NOT NULL) AS non_nulls FROM t_narrow_sparse_null;
SELECT min(value::Int64), max(value::Int64) FROM t_narrow_sparse_null WHERE value IS NOT NULL;

DROP TABLE t_narrow_sparse_null;
