-- Tags: no-fasttest

-- Safety checks for lazy JSON type-hint ALTERs (allow_experimental_json_lazy_type_hints).
-- A lazy metadata-only type-hint change skips the mutation branch, so it must be refused when it
-- changes the on-disk serialization of a value persisted positionally in the primary/partition
-- key or a secondary index (those are read back with the current type without per-part CAST).
-- The check is on the on-disk types of the subcolumns the key/index expression reads, not on the
-- expression result type: a type-sensitive expression such as reinterpretAsString(j.a) keeps the
-- same result type while its persisted bytes change. Changes on columns not feeding such a
-- structure, or that leave the on-disk type of every fed subcolumn unchanged, stay metadata-only.
-- Part of the series started in 04616_json_lazy_type_hints_key_safety, split for the
-- flaky-check runtime limit.

-- Read-in-order on the base table would decline the forced projections in this series
-- (`PROJECTION_NOT_USED`), so the series disables it: plan shape is not its subject.
SET optimize_read_in_order = 0;
SET enable_json_type = 1;
SET allow_experimental_json_lazy_type_hints = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_json_key_safety;

-- ============================================================
-- ALLOW: projection stores the whole JSON column but does not sort on the subcolumn.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'drop';
ALTER TABLE t_json_key_safety ADD PROJECTION p (SELECT id, j ORDER BY id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(16);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64);
SELECT 'projection on whole column -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'projection read after reload:', id, j.a FROM t_json_key_safety
ORDER BY id LIMIT 3 SETTINGS force_optimize_projection = 1;
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: projection sort key on the subcolumn -> primary.idx positionally persisted on j.a.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'drop';
ALTER TABLE t_json_key_safety ADD PROJECTION p (SELECT id, j ORDER BY j.a);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'projection sorts on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: projection sort key wraps the subcolumn in a type-sensitive reinterpretAsString(j.a).
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'drop';
ALTER TABLE t_json_key_safety ADD PROJECTION p (SELECT id, j ORDER BY reinterpretAsString(j.a));
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'projection sort key wraps subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: stored MATERIALIZED column derived from the subcolumn. A lazy change skips the
-- mutation that would recompute it, leaving stale on-disk bytes read back with no error.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32), k String MATERIALIZED reinterpretAsString(j.a))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'MATERIALIZED column on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: MATERIALIZED column reaching the subcolumn through an ALIAS (alias chain must be expanded).
-- ============================================================
CREATE TABLE t_json_key_safety
(id UInt32, j JSON(a Int32), a_alias String ALIAS reinterpretAsString(j.a), k String MATERIALIZED a_alias)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', -1)) FROM numbers(2);
SELECT 'MATERIALIZED column via ALIAS over subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: implicit auto-minmax index on an ALIAS column backed by the subcolumn.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32), k String ALIAS reinterpretAsString(j.a))
ENGINE = MergeTree ORDER BY id SETTINGS add_minmax_index_for_string_columns = 1;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'implicit index on ALIAS over subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: aggregate projection whose group key is an expression over the subcolumn. Defensive:
-- such projections cannot ingest subcolumn data yet, but the ALTER is refused all the same.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'drop';
ALTER TABLE t_json_key_safety ADD PROJECTION p (SELECT reinterpretAsString(j.a), count() GROUP BY reinterpretAsString(j.a));
SELECT 'aggregate projection group key on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: filtered projection whose WHERE reads the whole JSON column. Int32->Bool flips the render
-- ('{"a":0}' -> '{"a":false}'), so the WHERE result changes and the stored row set would go stale.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'drop';
ALTER TABLE t_json_key_safety ADD PROJECTION p (SELECT id, j WHERE toString(j) != '{"a":0}' ORDER BY id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number % 2)) FROM numbers(4);
SELECT 'filtered projection WHERE on json, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Bool); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW: total type change on a plain (non-key) column reads back correctly
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64);
SELECT 'plain column total change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'plain column total change, read after reload:', id, j.a FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW at ALTER time, FAIL on read: lossy type change on a plain column is not
-- validated at ALTER time (losslessness cannot be proven statically); the CAST
-- fails when the data is actually read.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety VALUES (1, '{"a": "hello"}');
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32);
SELECT 'lossy plain change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
SELECT j.a FROM t_json_key_safety; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t_json_key_safety;
