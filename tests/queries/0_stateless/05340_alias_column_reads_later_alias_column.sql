-- An ALIAS column that reads an ALIAS column declared after it must get that column's value as resolved in its own scope.

DROP TABLE IF EXISTS t_alias_order;

-- The alias `u` inside `c2` hides the column `u` only in `c2`.
CREATE TABLE t_alias_order (c0 Int64, u Int64, c3 Int64 ALIAS c2 * 10, c2 Int64 ALIAS (c0 AS u) + u) ENGINE = Memory;
INSERT INTO t_alias_order VALUES (1, 100);
SELECT c2, c3 FROM t_alias_order;
SELECT c2 FROM t_alias_order;
DROP TABLE t_alias_order;

-- The alias `u` inside `c3` is not visible in `c2`.
CREATE TABLE t_alias_order (c0 Int64, u Int64, c3 Int64 ALIAS c2 * 10 + (c0 * 1000 AS u), c2 Int64 ALIAS u + 1) ENGINE = Memory;
INSERT INTO t_alias_order VALUES (1, 100);
SELECT c2, c3 FROM t_alias_order;
DROP TABLE t_alias_order;

-- An alias used inside `c2` is resolved there.
CREATE TABLE t_alias_order (c0 Int64, c3 Int64 ALIAS c2 * 10, c2 Int64 ALIAS (c0 AS zz) + zz) ENGINE = Memory;
INSERT INTO t_alias_order VALUES (1);
SELECT c0, c2, c3 FROM t_alias_order;
DROP TABLE t_alias_order;

-- A chain of ALIAS columns read through a lambda.
CREATE TABLE t_alias_order (c0 Int64, u Int64, c4 Int64 ALIAS arrayMap(x -> x + c3, [1])[1], c3 Int64 ALIAS c2 * 10, c2 Int64 ALIAS (c0 AS u) + u) ENGINE = Memory;
INSERT INTO t_alias_order VALUES (1, 100);
SELECT c2, c3, c4 FROM t_alias_order;
DROP TABLE t_alias_order;
