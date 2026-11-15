-- A quoted identifier whose name contains a dot is one identifier, not a qualified reference, so
-- HAVING on the quoted alias `t1.c1` binds to the projection alias and not to column `c1` of the
-- table aliased `t1`. Enabling the identifier resolution cache must not change that.
-- The second case is the same rule with the dot in different places: `a.b`.c and a.`b.c` read the
-- same dotted name, both are qualified, and they denote different columns.
-- https://github.com/ClickHouse/ClickHouse/issues/121911

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 String, c1 Nullable(Float64)) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t1 SELECT toString(number), number / 4 FROM numbers(10);

SELECT '-- quoted dotted alias in HAVING --';
SELECT sum(t1.c1) AS `t1.c1` FROM t1 AS t1 HAVING `t1.c1` > 0
SETTINGS enable_identifier_resolve_cache = 0;
SELECT sum(t1.c1) AS `t1.c1` FROM t1 AS t1 HAVING `t1.c1` > 0
SETTINGS enable_identifier_resolve_cache = 1;

SELECT '-- equal part count, different boundaries --';
SELECT `a.b`.c, a.`b.c` FROM (SELECT 1 AS c) AS `a.b`, (SELECT 2 AS `b.c`) AS a
SETTINGS enable_identifier_resolve_cache = 0;
SELECT `a.b`.c, a.`b.c` FROM (SELECT 1 AS c) AS `a.b`, (SELECT 2 AS `b.c`) AS a
SETTINGS enable_identifier_resolve_cache = 1;

DROP TABLE t1;
