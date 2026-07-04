-- An unqualified column that resolves to both sides of an INNER JOIN is not ambiguous when the two
-- columns are equated by a top-level `equals` conjunct of the ON condition: every joined row then has
-- the same value on both sides. This must hold even with single_join_prefer_left_table = 0.

-- This is a property of the new analyzer; the old analyzer resolves ambiguity differently.
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

DROP TABLE t1;
DROP TABLE t2;
