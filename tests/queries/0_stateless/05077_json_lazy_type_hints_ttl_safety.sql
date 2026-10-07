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
-- REJECT: subcolumn feeds a rows TTL (DELETE) expression; ttl_infos would go stale.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(ts UInt32)) ENGINE = MergeTree ORDER BY id
TTL toDateTime(j.ts) + INTERVAL 1 DAY DELETE;
SELECT 'rows TTL on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(ts Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: subcolumn only in the WHERE of a rows TTL (the TTL time expression is on a plain column).
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, d DateTime, j JSON(ts UInt32)) ENGINE = MergeTree ORDER BY id
TTL d + INTERVAL 1 DAY DELETE WHERE j.ts > 0;
SELECT 'rows TTL WHERE on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(ts Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: subcolumn feeds a column TTL expression.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, x UInt32 TTL toDateTime(j.ts) + INTERVAL 1 DAY, j JSON(ts UInt32))
ENGINE = MergeTree ORDER BY id;
SELECT 'column TTL on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(ts Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: subcolumn feeds a GROUP BY TTL expression.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, v UInt32, j JSON(ts UInt32)) ENGINE = MergeTree ORDER BY id
TTL toDateTime(j.ts) + INTERVAL 1 DAY GROUP BY id SET v = max(v);
SELECT 'GROUP BY TTL on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(ts Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: subcolumn feeds only a GROUP BY ... SET assignment (stored in set_parts, not in the
-- time expression). The aggregated value column keeps stale bytes in already-aggregated parts.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, d DateTime, v String, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
TTL d + INTERVAL 1 DAY GROUP BY id SET v = argMax(reinterpretAsString(j.a), d);
SELECT 'GROUP BY SET TTL on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: subcolumn feeds a RECOMPRESS TTL expression.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(ts UInt32)) ENGINE = MergeTree ORDER BY id
TTL toDateTime(j.ts) + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1));
SELECT 'RECOMPRESS TTL on subcolumn, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(ts Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW: a guarded structure reads one subcolumn; changing a *different* existing
-- subcolumn's type leaves it intact, so the change must stay metadata-only.
-- ============================================================

-- ORDER BY reads j.a; change sibling j.b.
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32, b Int32)) ENGINE = MergeTree ORDER BY (j.a, id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number, 'b', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b Int64);
SELECT 'ORDER BY sibling change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'ORDER BY sibling change, read after reload:', id, j.a, j.b FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- PARTITION BY reads j.a; change sibling j.b.
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32, b Int32)) ENGINE = MergeTree PARTITION BY j.a ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number % 3, 'b', number)) FROM numbers(9);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b Int64);
SELECT 'PARTITION BY sibling change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'PARTITION BY sibling change, count j.b >= 4 after reload (expect 5):',
    count() FROM t_json_key_safety WHERE j.b >= 4;
DROP TABLE t_json_key_safety;

-- Skip index reads j.a; change sibling j.b.
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32, b Int32), INDEX idx j.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number, 'b', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b Int64);
SELECT 'skip index sibling change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'skip index sibling change, read after reload:', id, j.a, j.b FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- MATERIALIZED column reads j.a; change sibling j.b.
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32, b Int32), k String MATERIALIZED reinterpretAsString(j.a))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number, 'b', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b Int64);
SELECT 'MATERIALIZED sibling change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'MATERIALIZED sibling change, read after reload:', id, j.a, j.b FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- TTL reads j.ts; change sibling j.a. j.ts is far in the future so no row expires.
CREATE TABLE t_json_key_safety (id UInt32, j JSON(ts UInt32, a Int32)) ENGINE = MergeTree ORDER BY id
TTL toDateTime(j.ts) + INTERVAL 1 DAY DELETE;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('ts', 4000000000, 'a', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(ts UInt32, a Int64);
SELECT 'TTL sibling change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'TTL sibling change, read after reload:', id, j.a FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- GROUP BY SET assignment reads j.a; change sibling j.b (time expr is on plain column d).
CREATE TABLE t_json_key_safety (id UInt32, d DateTime, v String, j JSON(a Int32, b Int32)) ENGINE = MergeTree ORDER BY id
TTL d + INTERVAL 1 DAY GROUP BY id SET v = argMax(reinterpretAsString(j.a), d);
INSERT INTO t_json_key_safety SELECT number, now(), '', toJSONString(map('a', number, 'b', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b Int64);
SELECT 'GROUP BY SET sibling change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'GROUP BY SET sibling change, read after reload:', id, j.a, j.b FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- ============================================================
-- Setting off: the same change as the ORDER-BY-subcolumn case in
-- 04616_json_lazy_type_hints_key_safety goes through a full mutation
-- (existing non-lazy behavior), which forbids altering a key subcolumn.
-- ============================================================
SET allow_experimental_json_lazy_type_hints = 0;
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'setting off, key subcolumn change -> reject (mutation path):';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;
