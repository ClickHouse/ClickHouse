-- A `MODIFY COLUMN` subcommand changes the column default only when it carries one, or removes one.
-- Restating the type must leave the default alone, including the default a preceding subcommand of
-- the same ALTER has just set or removed.

DROP TABLE IF EXISTS t_mod_def;

-- 1. MATERIALIZED set, then a type-carrying CODEC subcommand.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'),
                      MODIFY COLUMN c UInt32 CODEC(T64, LZ4);
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '1', c FROM t_mod_def;
SELECT '1', default_kind, default_expression FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- 2. DEFAULT set, then a type-carrying CODEC subcommand.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 DEFAULT 7,
                      MODIFY COLUMN c UInt32 CODEC(T64, LZ4);
INSERT INTO t_mod_def (event) VALUES ('e');
SELECT '2', c FROM t_mod_def;
SELECT '2', default_kind, default_expression FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- 3. ALIAS set, then a type-carrying COMMENT subcommand.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 ALIAS length(event),
                      MODIFY COLUMN c UInt32 COMMENT 'cc';
SELECT '3', default_kind, default_expression, comment FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- 4. EPHEMERAL set, then a type-carrying COMMENT subcommand.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 EPHEMERAL 5,
                      MODIFY COLUMN c UInt32 COMMENT 'cc';
SELECT '4', default_kind, default_expression, comment FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- 5. REMOVE MATERIALIZED, then a type-carrying CODEC subcommand: the removal stands, so an explicit
--    INSERT into the column is accepted.
CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED,
                      MODIFY COLUMN c UInt32 CODEC(T64, LZ4);
SELECT '5', default_kind, default_expression FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
INSERT INTO t_mod_def (event, c) VALUES ('{"x":42}', 7);
SELECT '5', c FROM t_mod_def;
DROP TABLE t_mod_def;

-- 6. Three subcommands on one column: the default, the comment and the codec all survive.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'),
                      MODIFY COLUMN c UInt32 COMMENT 'cc',
                      MODIFY COLUMN c UInt32 CODEC(T64, LZ4);
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '6', c FROM t_mod_def;
SELECT '6', default_kind, comment, compression_codec FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- 7. The single-statement equivalent of case 1, and the spellings that were already correct.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 MATERIALIZED JSONExtractUInt(event, 'x') CODEC(T64, LZ4);
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '7single', c, default_kind FROM t_mod_def, system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'),
                      MODIFY COLUMN c CODEC(T64, LZ4);
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '7notype', c, default_kind FROM t_mod_def, system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED, MODIFY COLUMN c CODEC(T64, LZ4);
SELECT '7removenotype', default_kind FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- A real type change carries the column default across, and the stored data is converted.
CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
ALTER TABLE t_mod_def MODIFY COLUMN c UInt64;
INSERT INTO t_mod_def (event) VALUES ('{"x":43}');
SELECT '7typechange', c FROM t_mod_def ORDER BY c;
SELECT '7typechange', type, default_kind, default_expression FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32;
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '7restate', c, default_kind FROM t_mod_def, system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 CODEC(T64, LZ4);
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '7codeconly', c, default_kind FROM t_mod_def, system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- Chained ADD ENUM VALUES still composes.
CREATE TABLE t_mod_def (e Enum8('a' = 1)) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN e ADD ENUM VALUES('b' = 2), MODIFY COLUMN e ADD ENUM VALUES('c' = 3);
SELECT '7enum', type FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'e';
DROP TABLE t_mod_def;

-- 8. A real type change combined with a default set or removed in the same ALTER.
CREATE TABLE t_mod_def (event String, c UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'),
                      MODIFY COLUMN c UInt64;
INSERT INTO t_mod_def (event) VALUES ('{"x":42}');
SELECT '8set', c FROM t_mod_def;
SELECT '8set', type, default_kind, default_expression FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED, MODIFY COLUMN c UInt64;
SELECT '8remove', type, default_kind FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
INSERT INTO t_mod_def (event, c) VALUES ('{"x":42}', 7);
SELECT '8remove', c FROM t_mod_def;
DROP TABLE t_mod_def;

-- 9. The combined form reaches the same outcome as the two separate statements. Dropping a
--    MATERIALIZED that made the column nullable leaves a conversion the server cannot perform, and
--    that is reported either way.
CREATE TABLE t_mod_def (event String, c Nullable(UInt32) MATERIALIZED toUInt32OrNull(event))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED, MODIFY COLUMN c UInt32; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_mod_def;

CREATE TABLE t_mod_def (event String, c Nullable(UInt32) MATERIALIZED toUInt32OrNull(event))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED;
ALTER TABLE t_mod_def MODIFY COLUMN c UInt32; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_mod_def;

-- 10. Two type-carrying subcommands over a pre-existing default keep colliding as before.
CREATE TABLE t_mod_def (event String, c UInt32 MATERIALIZED JSONExtractUInt(event, 'x'))
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c UInt64 CODEC(T64, LZ4),
                      MODIFY COLUMN c UInt64 COMMENT 'z'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
DROP TABLE t_mod_def;

-- 11. An existing default that cannot be read as the new type is still rejected, with or without a
--     second subcommand, and whether or not the statements are combined.
CREATE TABLE t_mod_def (event String, c Enum8('x' = 1) MATERIALIZED 'x')
    ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED, MODIFY COLUMN c Int8; -- { serverError CANNOT_PARSE_TEXT }
ALTER TABLE t_mod_def MODIFY COLUMN c Int8; -- { serverError CANNOT_PARSE_TEXT }
ALTER TABLE t_mod_def MODIFY COLUMN c REMOVE MATERIALIZED;
ALTER TABLE t_mod_def MODIFY COLUMN c Int8;
SELECT '11', type, default_kind FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'c';
DROP TABLE t_mod_def;

-- 12. A version column keeps its MATERIALIZED default across a type restate.
CREATE TABLE t_mod_def (k UInt32, v UInt32) ENGINE = ReplacingMergeTree(v) ORDER BY k;
ALTER TABLE t_mod_def MODIFY COLUMN v UInt32 MATERIALIZED k + 1,
                      MODIFY COLUMN v UInt32 CODEC(T64, LZ4);
SELECT '12', default_kind, default_expression FROM system.columns
    WHERE database = currentDatabase() AND table = 't_mod_def' AND name = 'v';
ALTER TABLE t_mod_def MODIFY COLUMN v UInt32 ALIAS k; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_mod_def;
