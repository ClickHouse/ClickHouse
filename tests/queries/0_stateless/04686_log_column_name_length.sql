-- Tags: log-engine
-- The limit is a property of the filesystem holding the table, so the lengths below assume the
-- NAME_MAX = 255 that every filesystem CI runs on gives: a stream name of 251 plus `.bin` fits.

DROP TABLE IF EXISTS t_at_limit;
DROP TABLE IF EXISTS t_at_limit_tiny;
DROP TABLE IF EXISTS t_nested_at_limit;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_stripe;

-- 251 characters plus `.bin` is exactly one path component, so this still works.
CREATE TABLE t_at_limit (`ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = Log;
INSERT INTO t_at_limit VALUES (1);
SELECT count() FROM t_at_limit;

CREATE TABLE t_at_limit_tiny (`ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = TinyLog;
INSERT INTO t_at_limit_tiny VALUES (1);
SELECT count() FROM t_at_limit_tiny;

-- One character more does not fit, and is now refused at CREATE rather than at every INSERT. Each
-- rejection uses its own table name, so a CREATE that wrongly succeeds cannot be masked by
-- TABLE_ALREADY_EXISTS from the next one.
CREATE TABLE t_over_log (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }
CREATE TABLE t_over_tiny (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = TinyLog; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The name is escaped before it reaches the filesystem, so a much shorter identifier can overflow:
-- each `%` below is stored as `%25`, making 84 characters into 252.
CREATE TABLE t_escaped (`%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%` UInt8) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A substream appends to the column name, so the limit binds on the DERIVED name: `Nested` derives
-- both `%2Ea` and the shared `.size0`, and the longer of the two is why 245 fit here and 246 do not.
CREATE TABLE t_nested_at_limit (`ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` Nested(a UInt64)) ENGINE = Log;
INSERT INTO t_nested_at_limit VALUES ([1]);
SELECT count() FROM t_nested_at_limit;

CREATE TABLE t_nested_over (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` Nested(a UInt64)) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Columns that never reach the filesystem are not subject to the limit.
CREATE TABLE t_alias (x UInt8, `cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` ALIAS x + 1) ENGINE = Log;
INSERT INTO t_alias VALUES (1);
SELECT count() FROM t_alias;

-- StripeLog keeps every column in one shared file, so its names are not derived from the columns.
CREATE TABLE t_stripe (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = StripeLog;
INSERT INTO t_stripe VALUES (1);
SELECT count() FROM t_stripe;

-- An unsupported type is still refused as such, whatever the column is named.
CREATE TABLE t_variant (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` Variant(Int32)) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }

DROP TABLE t_at_limit;
DROP TABLE t_at_limit_tiny;
DROP TABLE t_nested_at_limit;
DROP TABLE t_alias;
DROP TABLE t_stripe;
