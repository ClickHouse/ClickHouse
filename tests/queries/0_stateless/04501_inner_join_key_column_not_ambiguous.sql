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

-- Null-safe equality (`<=>`, that is `isNotDistinctFrom`) carries a join key just like `equals` - see
-- the planner's key collection - and it is even stronger here: a surviving row has either the same
-- non-NULL value or `NULL` on both sides. So a key equated this way is not ambiguous either.
DROP TABLE IF EXISTS t_null_left;
DROP TABLE IF EXISTS t_null_right;

CREATE TABLE t_null_left (id Nullable(Int32), v Int32) ENGINE = Memory;
CREATE TABLE t_null_right (id Nullable(Int32), v Int32) ENGINE = Memory;
INSERT INTO t_null_left VALUES (1, 101), (2, 102), (NULL, 100);
INSERT INTO t_null_right VALUES (2, 102), (3, 203), (NULL, 100);

SELECT id FROM t_null_left INNER JOIN t_null_right ON t_null_left.id <=> t_null_right.id ORDER BY id NULLS LAST;

-- The same in an `and` chain, mixed with a plain `equals` conjunct.
SELECT id FROM t_null_left INNER JOIN t_null_right ON t_null_left.v = t_null_right.v AND t_null_left.id <=> t_null_right.id ORDER BY id NULLS LAST;

-- A non-key column is still ambiguous under a null-safe join key.
SELECT v FROM t_null_left INNER JOIN t_null_right ON t_null_left.id <=> t_null_right.id; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE t_null_left;
DROP TABLE t_null_right;

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

-- The relaxation matches a direct `equals` of the two resolved columns in the nearest join's ON; it
-- does not chase a transitive equivalence class through an intermediate table. In a multi-way chain
-- whose later ON equates a key that is not the exposed left representative, the reference stays
-- ambiguous by design: here the left subtree (t1 INNER JOIN t2) exposes `id` as `t1.id`, but the
-- second ON equates `t2.id` with `t3.id`, so a direct `t1.id = t3.id` is absent and no relaxation
-- applies. Resolving this would require following the equivalence class `t1.id = t2.id = t3.id`
-- across the left subtree, which is deliberately out of scope.
DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (id Int32, v Int32) ENGINE = Memory;
INSERT INTO t3 VALUES (2, 302), (3, 303), (5, 305);

SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id INNER JOIN t3 ON t2.id = t3.id; -- { serverError AMBIGUOUS_IDENTIFIER }

-- But when the later ON equates the exposed representative (t1.id) directly, it resolves as expected.
SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id INNER JOIN t3 ON t1.id = t3.id ORDER BY id;

-- The equality that justifies the relaxation must be a property of the join, independent of the
-- reference itself. An unqualified reference written inside the very ON that equates it - as in
-- `... INNER JOIN t3 ON id = t3.id` - is not covered: choosing a side there would change the join
-- condition itself (resolved to the right side the condition becomes trivially true), so the
-- "both sides carry the same value" argument does not apply, and the equated columns are not even
-- resolved yet at that point. Such a reference stays ambiguous exactly as before - in the multi-way
-- form regardless of single_join_prefer_left_table, and in the single-join form it keeps working only
-- through single_join_prefer_left_table = 1, which is untouched by this change.
SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id INNER JOIN t3 ON id = t3.id; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT id FROM t1 INNER JOIN t2 ON t1.id = t2.id INNER JOIN t3 ON id = t3.id SETTINGS single_join_prefer_left_table = 1; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT id FROM t1 INNER JOIN t2 ON id = t2.id; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT id FROM t1 INNER JOIN t2 ON id = t2.id ORDER BY id SETTINGS single_join_prefer_left_table = 1;

DROP TABLE t3;

DROP TABLE t1;
DROP TABLE t2;
