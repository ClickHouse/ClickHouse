-- An unqualified column that resolves to both sides of an INNER JOIN is not ambiguous when the two
-- columns are equated by a top-level `equals` conjunct of the ON condition: every joined row then has
-- the same value on both sides. This must hold even with single_join_prefer_left_table = 0.

-- This is a property of the analyzer; the old analyzer resolves ambiguity differently.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id Int32, v Int32) ENGINE = Memory;
CREATE TABLE t2 (id Int32, v Int32) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 101), (2, 102), (3, 103);
INSERT INTO t2 VALUES (2, 102), (3, 203), (4, 204);

SET single_join_prefer_left_table = 0;

-- Equated INNER JOIN key: not ambiguous.
SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY id;

-- The same holds with the default single_join_prefer_left_table = 1 (no regression).
SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY id SETTINGS single_join_prefer_left_table = 1;

-- A non-key column present on both sides is still ambiguous.
SELECT v FROM t1 INNER JOIN t2 ON t1.id = t2.id; -- { serverError AMBIGUOUS_IDENTIFIER }

-- When the column is itself equated, it is not ambiguous.
SELECT v FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t1.v = t2.v ORDER BY v;

-- Equality under OR does not guarantee equal values, so the key stays ambiguous.
SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id OR t1.v = t2.v; -- { serverError AMBIGUOUS_IDENTIFIER }

-- The relaxation is limited to INNER joins: an outer join may leave the non-preserved side default.
SELECT id FROM t1 LEFT JOIN t2 ON t1.id = t2.id; -- { serverError AMBIGUOUS_IDENTIFIER }

-- Explicit qualification always works.
SELECT t1.id FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY t1.id;

-- The relaxation is limited to unqualified (one-part) identifiers: a qualified or subcolumn path that
-- is ambiguous must keep raising the error, even when the ON equates the two candidates. Here `p.q` is
-- ambiguous between the left subcolumn `t_sub.p.q` and the right table `p` (aliased `pr`), and the ON
-- equates exactly those two - but the identifier is compound, so it stays ambiguous and is not silently
-- resolved to the left side.
DROP TABLE IF EXISTS t_sub;
DROP TABLE IF EXISTS p;

CREATE TABLE t_sub (p Tuple(q Int64)) ENGINE = Memory;
CREATE TABLE p (q Int32) ENGINE = Memory;
INSERT INTO t_sub VALUES ((1)), ((2));
INSERT INTO p VALUES (1), (2);

SELECT p.q FROM t_sub INNER JOIN p AS pr ON t_sub.p.q = pr.q; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE t_sub;
DROP TABLE p;

-- Mixed-type equi-keys: the join compares the keys after coercion to a common type, but the
-- unqualified reference resolves to the raw left column - the same value and type that the default
-- single_join_prefer_left_table = 1 has always produced for a single join. It does not become
-- a USING-like common-type projection.
DROP TABLE IF EXISTS t_narrow;
DROP TABLE IF EXISTS t_wide;

CREATE TABLE t_narrow (id UInt8, v Int32) ENGINE = Memory;
CREATE TABLE t_wide (id UInt16, v Int32) ENGINE = Memory;
INSERT INTO t_narrow VALUES (1, 101), (2, 102), (3, 103);
INSERT INTO t_wide VALUES (2, 202), (3, 203), (400, 204);

SELECT id, toTypeName(id) FROM t_narrow INNER JOIN t_wide ON t_narrow.id = t_wide.id ORDER BY id;

-- Identical to the pre-existing behavior with the default single_join_prefer_left_table = 1.
SELECT id, toTypeName(id) FROM t_narrow INNER JOIN t_wide ON t_narrow.id = t_wide.id ORDER BY id SETTINGS single_join_prefer_left_table = 1;

-- The same left-side semantics in a multi-way join, where single_join_prefer_left_table never applied.
SELECT id, toTypeName(id) FROM t_narrow INNER JOIN t_wide ON t_narrow.id = t_wide.id INNER JOIN t1 ON t_narrow.id = t1.id ORDER BY id;

-- USING exposes the common-type projection instead, unchanged by this relaxation.
SELECT id, toTypeName(id) FROM t_narrow INNER JOIN t_wide USING (id) ORDER BY id;

DROP TABLE t_narrow;
DROP TABLE t_wide;

DROP TABLE t1;
DROP TABLE t2;
