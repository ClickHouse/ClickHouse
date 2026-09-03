-- Tags: no-fasttest

-- Safety checks for lazy JSON type-hint ALTERs (allow_experimental_json_lazy_type_hints).
-- A lazy metadata-only type-hint change skips the mutation branch, so it must be refused when it
-- changes the on-disk serialization of a value persisted positionally in the primary/partition
-- key or a secondary index (those are read back with the current type without per-part CAST).
-- The check is on the on-disk types of the subcolumns the key/index expression reads, not on the
-- expression result type: a type-sensitive expression such as reinterpretAsString(j.a) keeps the
-- same result type while its persisted bytes change. Changes on columns not feeding such a
-- structure, or that leave the on-disk type of every fed subcolumn unchanged, stay metadata-only.

-- Read-in-order on the base table would decline the forced projections in this test
-- (`PROJECTION_NOT_USED`), so disable it: plan shape is not this test's subject.
SET optimize_read_in_order = 0;
SET enable_json_type = 1;
SET allow_experimental_json_lazy_type_hints = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_json_key_safety;

-- ============================================================
-- REJECT: typed-path subcolumn in ORDER BY, its type changes
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'ORDER BY subcolumn, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: typed-path subcolumn in PARTITION BY, its type changes
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree PARTITION BY j.a ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number % 3)) FROM numbers(9);
SELECT 'PARTITION BY subcolumn, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: explicit skip index on the typed-path subcolumn, its type changes
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32), INDEX idx j.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(8);
SELECT 'skip index on subcolumn, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW: add an unrelated typed path while the key is on an unchanged path
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id)
SETTINGS index_granularity = 4;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(64);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b String);
SELECT 'add unrelated path, key unchanged -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'add unrelated path, count j.a >= 40 after reload (expect 24):',
    count() FROM t_json_key_safety WHERE j.a >= 40 SETTINGS force_primary_key = 1;
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT (conservative): key wraps the subcolumn in a value-preserving function.
-- toString(j.a) yields the same string for the affected values, but losslessness
-- of the persisted key value cannot be proven statically, so once j.a changes
-- type the ALTER is rejected.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (toString(j.a), id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'toString(j.a) key, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: key wraps the subcolumn in a type-sensitive function whose result type
-- is stable (String) but whose persisted bytes change with the subcolumn type,
-- e.g. reinterpretAsString(j.a) writes 4 bytes for Int32 and 8 for Int64 (and a
-- different string for negatives). A result-type-only check wrongly allowed this
-- and corrupted primary.idx.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (reinterpretAsString(j.a), id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'reinterpretAsString(j.a) key, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT (conservative): skip index wraps the subcolumn in toString(j.a).
-- ============================================================
CREATE TABLE t_json_key_safety
(id UInt32, j JSON(a Int32), INDEX idx toString(j.a) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'toString(j.a) skip index, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: skip index wraps the subcolumn in a type-sensitive reinterpretAsString(j.a).
-- ============================================================
CREATE TABLE t_json_key_safety
(id UInt32, j JSON(a Int32), INDEX idx reinterpretAsString(j.a) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'reinterpretAsString(j.a) skip index, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

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
-- Setting off: the same change on a key subcolumn goes through a full mutation
-- (existing non-lazy behavior), which forbids altering a key subcolumn.
-- ============================================================
SET allow_experimental_json_lazy_type_hints = 0;
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'setting off, key subcolumn change -> reject (mutation path):';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;
