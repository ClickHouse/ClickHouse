-- Tags: no-parallel
-- (no-parallel because the assertion needs two cross-database tables with the same
--  table name to force the analyzer to keep the canonical projection name in
--  `db.table.column` form; database names are server-global, so the flaky check's
--  parallel iterations would race on `CREATE DATABASE` without this tag.)

-- Focused regression for the `db.table` source-name branch in `safe_short_name`
-- (https://github.com/ClickHouse/ClickHouse/pull/107449#discussion_r3483004360):
-- when two unaliased tables share the same table name across different databases,
-- `qualifyColumnNodesWithProjectionNames` emits canonical projection names like
-- `db1.t.f1`, and the short-name fallback's `TableNode` branch must accept
-- `database_name + "." + table_name` as a valid source-name match — otherwise the
-- outer query can't resolve the rightmost short name.

DROP DATABASE IF EXISTS db04411_a;
DROP DATABASE IF EXISTS db04411_b;
CREATE DATABASE db04411_a;
CREATE DATABASE db04411_b;
CREATE TABLE db04411_a.t (f1 UInt8) ENGINE = Memory;
CREATE TABLE db04411_b.t (f1 UInt8) ENGINE = Memory;
INSERT INTO db04411_a.t VALUES (10);
INSERT INTO db04411_b.t VALUES (20);

SET enable_analyzer = 1;
SET analyzer_enable_short_column_names_from_subquery = 1;
SET single_join_prefer_left_table = 0;

-- DESCRIBE confirms the canonical projection name retains the `db.table.column` form:
DESCRIBE (SELECT db04411_a.t.f1 FROM db04411_a.t, db04411_b.t);

-- The inner subquery puts BOTH unaliased `db04411_a.t` and `db04411_b.t` in scope so
-- the analyzer keeps the qualifier alive (as shown above), but the projection list
-- selects only one of them, so the outer short-name lookup `f1` is unambiguous and
-- exercises the `db.table` source-name match in `safe_short_name`. Without that
-- branch the outer `SELECT f1` returns `UNKNOWN_IDENTIFIER`; with it, the short name
-- resolves and we get `10`.
SELECT f1 FROM (
    SELECT db04411_a.t.f1 FROM db04411_a.t, db04411_b.t
);

-- Canonical (dotted) form also keeps working, as always:
SELECT `db04411_a.t.f1` FROM (
    SELECT db04411_a.t.f1 FROM db04411_a.t, db04411_b.t
);

DROP TABLE db04411_a.t;
DROP TABLE db04411_b.t;
DROP DATABASE db04411_a;
DROP DATABASE db04411_b;
