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

-- A name is re-parsed at fixed limits, so an element whose own nesting is deeper than what the stricter
-- limit of DataTypeFactory allows is checked like any other. Below that depth the elements here collide
-- just the same, and skipping them would have made the rejection depend on the build, because that
-- limit is lower under sanitizers.
CREATE TABLE t_collision_11 (k UInt8, v Variant(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))), Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }

-- Those fixed limits are not the session ones, and they must not be: none of the paths that accept a
-- type uses the session limits for it, so reading them here would let the check be stricter than the
-- statement which produced the type and skip exactly the colliding element. A trailing SETTINGS clause
-- is applied only after the statement has been parsed, so the statement below is parsed at the default
-- depth while a session driven re-parse would run at 20 and fail on these elements. The clause has to
-- be written on the statement: a preceding SET makes the statement itself unparseable.
CREATE TABLE t_collision_12 (k UInt8, v Variant(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64))))))))))))))))), Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))))))))))))))))) ENGINE = MergeTree ORDER BY k SETTINGS max_parser_depth = 20; -- { serverError ILLEGAL_COLUMN }

-- A CAST target type never passes through the parser of a statement at all, so it needs its own case.
SELECT CAST(NULL, 'Variant(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64))))))))))))))))), Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))))))))))))))))') SETTINGS max_parser_depth = 20; -- { serverError ILLEGAL_COLUMN }

-- The size limit is the other way the re-parse could be the stricter one: it is a constant everywhere,
-- so an element name longer than it would never even be tokenized. The pair below renders to names of
-- about 269000 bytes each, which is past that constant, and it is built by a query rather than spelled
-- out to keep this file small. A table function is used because it reaches the same check while taking
-- its structure as a value.
DESC (SELECT * FROM format(TSV, (SELECT 'v Variant(' ||
    'AggregateFunction(0, sumMapFiltered([' || arrayStringConcat(range(1, 40001), ', ') || ']), Array(UInt64), Array(UInt64)), ' ||
    'AggregateFunction(1, sumMapFiltered([' || arrayStringConcat(range(1, 40001), ', ') || ']), Array(UInt64), Array(UInt64)))'), ''))
SETTINGS max_query_size = 10000000; -- { serverError ILLEGAL_COLUMN }

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

-- The values table function treats a structure string it cannot validate as row data instead of
-- refusing it, because tryParseColumnsListFromString reports any post-parse failure as "this is not
-- a column list". That fallback is pre-existing and shared with every other check in
-- validateDataType, so it is pinned here rather than changed.
SELECT * FROM values('Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))', 'x');

-- The source and the target of the materialized view cases below.
CREATE TABLE mv_src (k UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mv_tgt (k UInt8, v UInt8) ENGINE = MergeTree ORDER BY k;

-- The check is an integrity requirement, not a suspicious type policy, so it is not gated by any
-- setting. A nested Variant is created with no opt-in at all when the traversal of nested types is
-- disabled, so the rejection must hold there as well.
SET allow_suspicious_variant_types = 0, validate_experimental_and_suspicious_types_inside_nested_types = 0;
CREATE TABLE t_collision_7 (k UInt8, v Array(Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_collision_8 (k UInt8, v Map(String, Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))))) ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_COLUMN }
-- Only the ungated check covers a materialized view: the suspicious type policy stays exempt for one,
-- so the type the plain table above is refused for is still accepted here. This is the line that fails
-- if the exemption is dropped from the gated branch as well.
CREATE MATERIALIZED VIEW mv_gated TO mv_tgt (k UInt8, v Variant(UInt8, Int64)) AS SELECT k, 1 AS v FROM mv_src;
SELECT 'mv_gated', type FROM system.columns WHERE database = currentDatabase() AND table = 'mv_gated' AND name = 'v';
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
    c18 AggregateFunction(any, Variant(String, UInt8)),
    -- A deeply nested element, to pin that a type is not refused just for being deeper than the limit
    -- DataTypeFactory applies to a name given as a string, which is lower under sanitizers than it is
    -- elsewhere and lower than the limit this statement was parsed with.
    c19 Variant(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(UInt8))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))), String)
) ENGINE = MergeTree ORDER BY tuple();
SELECT 'controls', count() FROM system.columns WHERE database = currentDatabase() AND table = 't_control';
SELECT 'c11', type FROM system.columns WHERE database = currentDatabase() AND table = 't_control' AND name = 'c11';
SELECT 'c12', type FROM system.columns WHERE database = currentDatabase() AND table = 't_control' AND name = 'c12';

-- An ATTACH that carries a whole table definition creates and persists a new table, so its columns are
-- fresh input rather than a definition this server stored earlier, and the same type is refused there
-- too. A Memory database is used because that spelling needs a database which does not assign UUIDs.
-- That CREATE DATABASE line also matches one of the restricted functionality patterns of the cloud
-- test prefilter, so the whole file is skipped there by design and not because of a regression.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_attached (k UInt8, v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The column list of a fresh materialized view is persisted verbatim and parsed back on reload just
-- like a table's, so the same type is refused there too. The view writes into an existing target, which
-- is the spelling that creates no inner table, and the colliding type is carried only by that column
-- list: nothing in the SELECT produces it.
CREATE MATERIALIZED VIEW mv_collision TO mv_tgt (k UInt8, v Variant(AggregateFunction(0, sumMap, Array(UInt64), Array(UInt64)), AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)))) AS SELECT k, 1 AS v FROM mv_src; -- { serverError ILLEGAL_COLUMN }

-- A materialized view whose element names stay distinct is created, and so is one with an inner table.
CREATE MATERIALIZED VIEW mv_ok TO mv_tgt (k UInt8, v Variant(AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)), UInt8)) AS SELECT k, 1 AS v FROM mv_src;
SELECT 'mv_ok', type FROM system.columns WHERE database = currentDatabase() AND table = 'mv_ok' AND name = 'v';
CREATE MATERIALIZED VIEW mv_inner ENGINE = MergeTree ORDER BY k AS SELECT k FROM mv_src;
SELECT 'mv_inner', count() FROM system.columns WHERE database = currentDatabase() AND table = 'mv_inner';

-- A definition stored by this server is not re-checked, so tables that already exist keep loading: the
-- short form of ATTACH remains exempt from data type validation entirely. The colliding type can no
-- longer be reached through any accepted path, so this uses a type that the settings gated checks would
-- refuse instead, which pins the exemption just as well.
CREATE TABLE t_reattached (k UInt8, v Variant(UInt8, Int64)) ENGINE = MergeTree ORDER BY k;
SET allow_suspicious_variant_types = 0;
DETACH TABLE t_reattached;
ATTACH TABLE t_reattached;
SET allow_suspicious_variant_types = 1;
SELECT 'reattached', type FROM system.columns WHERE database = currentDatabase() AND table = 't_reattached' AND name = 'v';
DROP TABLE t_reattached;

DROP TABLE mv_ok;
DROP TABLE mv_inner;
DROP TABLE mv_gated;
DROP TABLE mv_src;
DROP TABLE mv_tgt;
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
DROP TABLE IF EXISTS t_collision_11;
DROP TABLE IF EXISTS t_collision_12;
DROP TABLE IF EXISTS mv_collision;
