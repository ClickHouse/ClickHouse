-- Tags: no-replicated-database
-- Tag no-replicated-database: Unsupported type of CREATE TABLE ... CLONE AS ... query

-- Regression test for: `CLONE AS SELECT` silently ignores `CLONE` keyword instead of throwing.
-- See https://github.com/ClickHouse/ClickHouse/issues/104994
--
-- `CLONE AS` only makes sense when the source is a real table on disk (the partition-attach
-- step needs partitions). `CLONE AS SELECT ...` and `CLONE AS <table_function>` are accepted
-- by the parser but used to silently fall back to plain `CREATE ... AS SELECT`. Both forms
-- must now be rejected with `BAD_ARGUMENTS`, for a fresh `CREATE` as well as for a
-- user-supplied full `ATTACH` definition (which carries an explicit schema and goes through
-- the create-like path).

DROP TABLE IF EXISTS clone_src;
DROP TABLE IF EXISTS clone_dst;

CREATE TABLE clone_src (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO clone_src VALUES (1), (2), (3);

-- Sanity: regular CLONE AS <table> still works.
CREATE TABLE clone_dst CLONE AS clone_src;
SELECT * FROM clone_dst ORDER BY x;
DROP TABLE clone_dst;

-- CLONE AS SELECT must be rejected.
CREATE TABLE clone_dst CLONE AS SELECT x * 2 AS doubled FROM clone_src; -- { serverError BAD_ARGUMENTS }

-- CLONE AS SELECT with an explicit column list + engine must also be rejected.
-- (The parser only attempts SELECT in this branch when a storage clause is present, so include ENGINE.)
CREATE TABLE clone_dst (doubled Int64) ENGINE = MergeTree ORDER BY doubled CLONE AS SELECT x * 2 AS doubled FROM clone_src; -- { serverError BAD_ARGUMENTS }

-- CLONE AS <table_function> must be rejected.
CREATE TABLE clone_dst CLONE AS numbers(10); -- { serverError BAD_ARGUMENTS }

-- The same must hold for a user-supplied full ATTACH definition. A full ATTACH (explicit column
-- list, and an engine for the SELECT form) is not the short-attach path: `attach_short_syntax` is
-- false, so it goes through the create-like path and would otherwise silently drop the `CLONE`.
ATTACH TABLE clone_dst (doubled Int64) ENGINE = MergeTree ORDER BY doubled CLONE AS SELECT x * 2 AS doubled FROM clone_src; -- { serverError BAD_ARGUMENTS }

-- Full ATTACH with a table function (an engine cannot be specified for table functions).
ATTACH TABLE clone_dst (n UInt64) CLONE AS numbers(10); -- { serverError BAD_ARGUMENTS }

DROP TABLE clone_src;
