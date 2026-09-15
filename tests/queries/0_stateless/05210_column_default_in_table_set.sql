-- https://github.com/ClickHouse/ClickHouse/issues/117276
-- A column definition whose expression is a bare `IN <table>` membership test - `flag UInt8 ALIAS p
-- IN keys`, or the same with `DEFAULT` / `MATERIALIZED` - is accepted at CREATE time, because a bare
-- table identifier is not a subquery node and so passes the validation that rejects subqueries in
-- column defaults. Every consumer of the definition then has to compute the set, instead of
-- reporting `Not-ready Set is passed as the second argument for function 'in'`.

DROP TABLE IF EXISTS t_default_in_set_keys;
DROP TABLE IF EXISTS t_default_in_set_alias;
DROP TABLE IF EXISTS t_default_in_set_default;
DROP TABLE IF EXISTS t_default_in_set_materialized;
DROP TABLE IF EXISTS t_default_in_set_engine_keys;
DROP TABLE IF EXISTS t_default_in_set_engine;

CREATE TABLE t_default_in_set_keys (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_in_set_keys VALUES (1);

CREATE TABLE t_default_in_set_alias (p UInt8, x UInt64, flag UInt8 ALIAS p IN t_default_in_set_keys)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_default_in_set_alias (p, x) VALUES (1, 1), (2, 2);

SELECT 'ALIAS in WHERE';
SELECT p FROM t_default_in_set_alias WHERE flag;

SELECT 'ALIAS read';
SELECT x, flag FROM t_default_in_set_alias ORDER BY x;

-- The set of an `ALIAS` column referenced from inside a subquery is registered while the column is
-- expanded rather than by the outer `collectSets`, and still has to be built.
SELECT 'ALIAS referenced from a subquery';
SELECT x FROM (SELECT x, flag FROM t_default_in_set_alias) WHERE flag ORDER BY x;

SELECT 'mutation over the ALIAS carrier';
ALTER TABLE t_default_in_set_alias DELETE WHERE flag SETTINGS mutations_sync = 1;
SELECT x FROM t_default_in_set_alias ORDER BY x;

SELECT 'DEFAULT on INSERT';
CREATE TABLE t_default_in_set_default (p UInt8, f UInt8 DEFAULT p IN t_default_in_set_keys)
ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_in_set_default (p) VALUES (1), (2);
SELECT p, f FROM t_default_in_set_default ORDER BY p;

SELECT 'MATERIALIZED on INSERT';
CREATE TABLE t_default_in_set_materialized (p UInt8, f UInt8 MATERIALIZED p IN t_default_in_set_keys)
ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_in_set_materialized (p) VALUES (1), (2);
SELECT p, f FROM t_default_in_set_materialized ORDER BY p;

-- An `ALIAS` is computed on read, so it follows the current contents of the set, while `DEFAULT` and
-- `MATERIALIZED` keep the value computed when the row was inserted.
INSERT INTO t_default_in_set_keys VALUES (2);

SELECT 'ALIAS after the set grew';
SELECT x, flag FROM t_default_in_set_alias ORDER BY x;
SELECT 'DEFAULT after the set grew';
SELECT p, f FROM t_default_in_set_default ORDER BY p;
SELECT 'MATERIALIZED after the set grew';
SELECT p, f FROM t_default_in_set_materialized ORDER BY p;

-- A `Set` engine table on the right-hand side is a ready set rather than a deferred one, and worked
-- all along.
SELECT 'Set engine on the right';
CREATE TABLE t_default_in_set_engine_keys (p UInt8) ENGINE = Set;
INSERT INTO t_default_in_set_engine_keys VALUES (1);
CREATE TABLE t_default_in_set_engine (p UInt8, flag UInt8 ALIAS p IN t_default_in_set_engine_keys)
ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_in_set_engine (p) VALUES (1), (5);
SELECT p, flag FROM t_default_in_set_engine ORDER BY p;

-- An explicit subquery in a column default stays rejected at CREATE time.
CREATE TABLE t_default_in_set_subquery (p UInt8, flag UInt8 ALIAS p IN (SELECT p FROM t_default_in_set_keys))
ENGINE = MergeTree ORDER BY p; -- { serverError THERE_IS_NO_DEFAULT_VALUE }

DROP TABLE t_default_in_set_engine;
DROP TABLE t_default_in_set_engine_keys;
DROP TABLE t_default_in_set_materialized;
DROP TABLE t_default_in_set_default;
DROP TABLE t_default_in_set_alias;
DROP TABLE t_default_in_set_keys;
