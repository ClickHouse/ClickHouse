-- A Variant whose elements render to the same type name once that name is parsed back loses all but
-- one of them on reload, because the canonical Variant constructor deduplicates elements by name.
-- Such a type is refused at creation. Version 0 is omitted from an AggregateFunction name and is
-- resolved to the default version when the name is parsed again, which is how two distinct elements
-- end up sharing a name.

SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_alter_add;
DROP TABLE IF EXISTS t_alter_modify;
DROP TABLE IF EXISTS t_control;

-- The two versioned aggregate function classes, plus any combinator over them, at the top level. Every
-- rejected statement uses its own table name so that a run against a build without the fix reports each
-- case separately instead of failing on the leftover table of the previous one.
CREATE TABLE t_collision_1 (k UInt8, v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_2 (k UInt8, v Variant(AggregateFunction(0, minMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, minMap, Array(UInt64), Array(UInt64)))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_3 (k UInt8, v Variant(AggregateFunction(0, groupBitmapOr, AggregateFunction(groupBitmap, UInt32)), AggregateFunction(1, groupBitmapOr, AggregateFunction(groupBitmap, UInt32)))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }

-- Nested inside another type.
CREATE TABLE t_collision_4 (k UInt8, v Array(Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_5 (k UInt8, v Map(String, Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_6 (k UInt8, v Tuple(a Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }

-- The argument of an aggregate function is part of the stored name as well, and it is not reachable
-- through the child traversal every other check here relies on, so it gets its own cases: one at the
-- top level and one behind a wrapper, which also pins that the walk descends through a wrapper into an
-- aggregate argument.
CREATE TABLE t_collision_9 (k UInt8, v AggregateFunction(any, Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_10 (k UInt8, v Array(AggregateFunction(any, Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }

-- ALTER reaches the same stored metadata, so it is refused too. The added and the modified column
-- live on separate tables so that a build which still accepts the ADD does not then fail the whole
-- run on a duplicate column name.
CREATE TABLE t_alter_add (k UInt8) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_alter_add ADD COLUMN v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_alter_modify (k UInt8, v Variant(AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_alter_modify MODIFY COLUMN v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))); -- { serverError ILLEGAL_COLUMN }
SELECT 'alter_add', count() FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_add' AND name = 'v';
SELECT 'alter_modify', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_modify' AND name = 'v';

-- A CAST to such a type is refused as well: it yields a value whose type cannot be persisted.
SELECT CAST(NULL, 'Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))'); -- { serverError ILLEGAL_COLUMN }
DESC (SELECT * FROM format(TSV, 'v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))', '')); -- { serverError ILLEGAL_COLUMN }

-- The check is an integrity requirement, not a suspicious type policy, so it is not gated by any
-- setting. A nested Variant is created with no opt-in at all when the traversal of nested types is
-- disabled, so the rejection must hold there as well.
SET allow_suspicious_variant_types = 0, validate_experimental_and_suspicious_types_inside_nested_types = 0;
CREATE TABLE t_collision_7 (k UInt8, v Array(Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_8 (k UInt8, v Map(String, Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
SET allow_suspicious_variant_types = 1, validate_experimental_and_suspicious_types_inside_nested_types = 1;

-- Every Variant whose element names stay distinct keeps working, including a version 0 element paired
-- with a different function and a non default explicit version. Those must not be refused here,
-- because they keep both elements addressable and every read of them works. They do change on reload,
-- and by more than the printed version: an element pinned to version 0 sorts under the first letter of
-- its function name at creation and under '1' after the reload resolves it to the default version, so
-- its sort position, and therefore its persisted discriminator, can move. c10 and c11 below swap their
-- two discriminators across a restart. That is the same remap that makes printing version 0 in the name
-- unusable as a fix, it is a separate defect from the element loss this test covers, and only a
-- load-aware parse removes it.
CREATE TABLE t_control (
    c1 Variant(String, UInt8),
    c2 Variant(AggregateFunction(uniq, UInt64), UInt8),
    c3 Variant(AggregateFunction(quantiles(0.5, 0.9), UInt64), UInt8),
    c4 Variant(AggregateFunction(anyIf, String, UInt8), UInt8),
    c5 Variant(SimpleAggregateFunction(sum, UInt64), String),
    c6 Variant(DateTime('UTC'), Date),
    c7 Variant(Enum8('a' = 1), Enum8('b' = 1)),
    c8 Variant(Decimal32(2), Decimal(9, 2)),
    c9 Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), UInt8),
    c10 Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(avg, Int64)),
    c11 Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(7, sumMap, Array(UInt64), Array(UInt64))),
    c12 Variant(AggregateFunction(0, groupBitmap, UInt32), AggregateFunction(1, groupBitmap, UInt32)),
    c13 Point,
    c14 Ring,
    c15 Geometry,
    c16 Dynamic,
    c17 Array(Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), UInt8)),
    c18 AggregateFunction(any, Variant(String, UInt8))
) ENGINE = MergeTree ORDER BY tuple();
SELECT 'controls', count() FROM system.columns WHERE database = currentDatabase() AND table = 't_control';
SELECT 'c11', type FROM system.columns WHERE database = currentDatabase() AND table = 't_control' AND name = 'c11';
SELECT 'c12', type FROM system.columns WHERE database = currentDatabase() AND table = 't_control' AND name = 'c12';

-- Tables that already contain such a column keep loading exactly as before: data type validation is
-- skipped for ATTACH, so no stored schema is re-checked against the new rule. A Memory database is
-- used because a plain ATTACH with a column list needs a database that does not assign UUIDs. That
-- CREATE DATABASE line also matches one of the restricted functionality patterns of the cloud test
-- prefilter, so the whole file is skipped there by design and not because of a regression.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_attached (k UInt8, v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))) ENGINE = Memory;
SELECT 'attached', type FROM system.columns WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_attached' AND name = 'v';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE t_alter_add;
DROP TABLE t_alter_modify;
DROP TABLE t_control;

-- No-ops with the fix in place: they only clean up after a run against a build that still accepts
-- these types, so such a run reports the missing errors rather than a leftover table.
DROP TABLE IF EXISTS t_collision_1;
DROP TABLE IF EXISTS t_collision_2;
DROP TABLE IF EXISTS t_collision_3;
DROP TABLE IF EXISTS t_collision_4;
DROP TABLE IF EXISTS t_collision_5;
DROP TABLE IF EXISTS t_collision_6;
DROP TABLE IF EXISTS t_collision_7;
DROP TABLE IF EXISTS t_collision_8;
DROP TABLE IF EXISTS t_collision_9;
DROP TABLE IF EXISTS t_collision_10;
