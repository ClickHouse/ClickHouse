-- https://github.com/ClickHouse/ClickHouse/issues/116906
-- `RENAME DATABASE` used to move an `Alias` table into its own target namespace without
-- re-validating the stored engine arguments, leaving `db.t = Alias(db, t)` behind. That cycle in the
-- server-wide table dependency graph then made every unrelated DDL statement that adds a dependency
-- edge fail with `INFINITE_LOOP`.

DROP DATABASE IF EXISTS db_05072_from;
DROP DATABASE IF EXISTS db_05072_to;
CREATE DATABASE db_05072_from;

-- The target does not exist yet, so the self-reference check at CREATE passes vacuously.
CREATE TABLE db_05072_from.t ENGINE = Alias('db_05072_to', 't');

RENAME DATABASE db_05072_from TO db_05072_to; -- { serverError INFINITE_LOOP }

-- The database is untouched and unrelated DDL still works.
SELECT name, engine FROM system.tables WHERE database = 'db_05072_from' ORDER BY name;

DROP TABLE IF EXISTS t_05072_base;
CREATE TABLE t_05072_base (x Int32) ENGINE = MergeTree ORDER BY x;
CREATE VIEW v_05072 AS SELECT x FROM t_05072_base;
SELECT count() FROM v_05072;

-- A two-table cycle is rejected as well.
DROP DATABASE IF EXISTS db_05072_pair;
CREATE DATABASE db_05072_pair;
CREATE TABLE db_05072_pair.a ENGINE = Alias('db_05072_paired', 'b');
CREATE TABLE db_05072_pair.b ENGINE = Alias('db_05072_paired', 'a');
RENAME DATABASE db_05072_pair TO db_05072_paired; -- { serverError INFINITE_LOOP }

-- A rename that does not create a cycle still works.
DROP DATABASE IF EXISTS db_05072_ok;
DROP DATABASE IF EXISTS db_05072_ok_renamed;
CREATE DATABASE db_05072_ok;
CREATE TABLE db_05072_ok.t ENGINE = Alias(currentDatabase(), 't_05072_base');
RENAME DATABASE db_05072_ok TO db_05072_ok_renamed;
SELECT count() FROM db_05072_ok_renamed.t;

DROP DATABASE db_05072_from;
DROP DATABASE db_05072_pair;
DROP DATABASE db_05072_ok_renamed;
DROP VIEW v_05072;
DROP TABLE t_05072_base;
