-- Tags: log-engine
-- 251 plus `.bin` is exactly one path component on the NAME_MAX = 255 that CI runs on.

DROP TABLE IF EXISTS t_at_limit;
DROP TABLE IF EXISTS t_nested_at_limit;

CREATE TABLE t_at_limit (`ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = Log;
INSERT INTO t_at_limit VALUES (1);
SELECT count() FROM t_at_limit;

CREATE TABLE t_over (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }
CREATE TABLE t_over_tiny (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = TinyLog; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The name is escaped first, so a much shorter identifier overflows: each `%` becomes `%25`.
CREATE TABLE t_escaped (`%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%` UInt8) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A substream appends to the name, so the limit binds on the derived name, not the column.
CREATE TABLE t_nested_at_limit (`ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` Nested(a UInt64)) ENGINE = Log;
INSERT INTO t_nested_at_limit VALUES ([1]);
SELECT count() FROM t_nested_at_limit;
CREATE TABLE t_nested_over (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` Nested(a UInt64)) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- StripeLog keeps every column in one shared file, so its names are not column-derived.
CREATE TABLE t_stripe (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = StripeLog;
INSERT INTO t_stripe VALUES (1);
SELECT count() FROM t_stripe;
DROP TABLE t_stripe;

-- A full-definition ATTACH is fresh user input, unlike a short ATTACH of stored metadata.
ATTACH TABLE t_attach_over UUID '00000000-0000-0000-0000-0000000004a2' (`cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt8) ENGINE = Log; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- RENAME does not touch a column name, and the limit is per path component, so the at-limit
-- table stays writable however long the table and database names become.
RENAME TABLE t_at_limit TO t_at_limit_renamed_to_something_very_much_longer_than_before;
INSERT INTO t_at_limit_renamed_to_something_very_much_longer_than_before VALUES (2);
SELECT count() FROM t_at_limit_renamed_to_something_very_much_longer_than_before;

DROP TABLE t_at_limit_renamed_to_something_very_much_longer_than_before;
DROP TABLE t_nested_at_limit;
