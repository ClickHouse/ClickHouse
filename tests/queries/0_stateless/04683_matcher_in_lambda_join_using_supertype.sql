-- An unqualified matcher inside a lambda body in PREWHERE over JOIN USING must expand to each
-- table's own columns, exactly like the same matcher written directly in PREWHERE. Before the fix
-- it expanded to the merged USING key instead, so the header declared the supertype while the read
-- supplied the table's own column, and the server aborted with a LOGICAL_ERROR.
--
-- The oracle is the DIRECT (non-lambda) matcher in PREWHERE, which is correct on master. It is read
-- through EXPLAIN QUERY TREE, never through toTypeName: toTypeName constant-folds during analysis,
-- before the PREWHERE type rollback runs, so it reports the pre-rollback type for both forms.

SET enable_analyzer = 1;

-- 1. The wrapper-free carrier: UInt32 vs UInt64. No LowCardinality, no Nullable.

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;
CREATE TABLE r1 (`a` UInt32, `v` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE r2 (`a` UInt64, `w` UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO r1 VALUES (1, 10);
INSERT INTO r2 VALUES (1, 11);

SELECT 'int inner';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;
SELECT 'int inner direct oracle';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE ((tuple(* EXCEPT w)).2) != 0;

-- The load-bearing assertion: the lambda expansion must have the SAME type as the direct one.
-- Both must print 1.
SELECT 'int lambda expansion equals direct expansion', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(UInt32, Int16, UInt32)%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0);
SELECT 'int direct expansion', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(UInt32, Int16, UInt32)%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE ((tuple(* EXCEPT w)).2) != 0);

SELECT 'int left';
SELECT w, a, v FROM r1 LEFT JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;
SELECT 'int right';
SELECT w, a, v FROM r1 RIGHT JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;

-- FULL JOIN did not abort before the fix, but the lambda body expanded to the merged key while the
-- direct form kept the table's own column. Pin the type-level agreement only; the values agree on
-- both sides, so there is no wrong-answer claim here.
SELECT 'int full lambda expansion equals direct expansion', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(UInt32, Int16, UInt32)%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT w, a, v FROM r1 FULL JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0);
SELECT 'int full direct expansion', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(UInt32, Int16, UInt32)%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT w, a, v FROM r1 FULL JOIN r2 USING (a) PREWHERE ((tuple(* EXCEPT w)).2) != 0);

-- 5. Every higher-order function reaches this through the one shared lambda resolution path.
SELECT 'arrayFilter';
SELECT length(arrayFilter(z -> ((tuple(* EXCEPT w).2) != 0), [1])) FROM r1 INNER JOIN r2 USING (a)
PREWHERE (arrayFilter(z -> ((tuple(* EXCEPT w).2) != 0), [1])[1]) = 1;
SELECT 'arrayExists';
SELECT 1 FROM r1 INNER JOIN r2 USING (a) PREWHERE arrayExists(z -> ((tuple(* EXCEPT w).2) != 0), [1]);
SELECT 'arraySort';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE (arraySort(z -> (tuple(* EXCEPT w).2), [1])[1]) = 1;
SELECT 'nested lambda';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a)
PREWHERE ((arrayMap(y -> (arrayMap(z -> tuple(* EXCEPT w), [1])[1]), [1])[1]).2) != 0;

-- 6. Matcher spellings and USING shapes.
SELECT 'using alias';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a AS a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;
SELECT 'COLUMNS matcher';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(COLUMNS('a|v')), [1])[1]).2) != 0;
SELECT 'natural join';
SELECT a, v FROM r1 NATURAL INNER JOIN r2 PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;

-- A bare multi-column matcher in a single-argument lambda position is a user error, the same one a
-- join-free query reports. Before the fix it aborted the server instead.
SELECT 'multi column matcher in one argument lambda';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a)
PREWHERE ((arrayMap(z -> (* EXCEPT (v, w)), [1])[1]).2) != 0; -- { serverError UNSUPPORTED_METHOD }

-- 8. Already correct before the fix, must stay unchanged.
SELECT 'WHERE keeps the supertype';
SELECT toTypeName(arrayMap(z -> tuple(* EXCEPT w), [1])) FROM r1 INNER JOIN r2 USING (a) WHERE v != 0;
SELECT 'qualified matcher';
SELECT w, r1.a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE ((arrayMap(z -> tuple(r1.*), [1])[1]).1) != 0;
SELECT 'explicit columns';
SELECT toTypeName(arrayMap(z -> tuple(a, v), [1])) FROM r1 INNER JOIN r2 USING (a) PREWHERE v != 0;
SELECT 'bare key column';
SELECT w, a, v FROM r1 INNER JOIN r2 USING (a) PREWHERE (arrayMap(z -> a, [1])[1]) = 1;
SELECT 'matcher excluding the key';
SELECT toTypeName(arrayMap(z -> tuple(* EXCEPT (a, w)), [1])) FROM r1 INNER JOIN r2 USING (a) PREWHERE v != 0;
SELECT 'APPLY transformer';
SELECT toTypeName(arrayMap(z -> tuple(* EXCEPT w APPLY toString), [1])) FROM r1 INNER JOIN r2 USING (a) PREWHERE v != 0;
SELECT 'matcher in the select list';
SELECT toTypeName(arrayMap(z -> tuple(* EXCEPT w), [1])) FROM r1 INNER JOIN r2 USING (a);
-- With analyzer_compatibility_join_using_top_level_identifier the USING key comes from the SELECT
-- projection, so the helper returns before touching the matched column's type. Read the resolved
-- expansion directly: it must stay at the projection's type on both sides of the fix.
SELECT 'top level identifier compatibility', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(UInt64, Int16)%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT a + 1 AS a, arrayMap(z -> tuple(* EXCEPT w), [1]) FROM r1 INNER JOIN r2 USING (a)
    SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1);

-- 9. The rollback now descends into lambda bodies for every writer of the replacement map, so cover
-- the other channels too: the nullability writer, an empty map, a lambda argument, and a lambda
-- subquery. The nullability cases need a NON-PRESERVED side, so the lambda body reads a column the
-- join actually widens: under LEFT JOIN both `a` and `v` come from the preserved left table and stay
-- non-Nullable, which is why the supertype carrier below is the only thing a LEFT shape can assert.
SELECT 'join_use_nulls with USING';
SELECT count() FROM r1 LEFT JOIN r2 USING (a)
PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0 SETTINGS join_use_nulls = 1;
-- RIGHT/FULL make the LEFT table's `v` the non-preserved one, so the map really carries a
-- Nullable(Int16) entry for it. Assert the resolved type, not just the row count: the lambda body
-- must converge on the direct form's Int16 after the rollback, and print 1 twice.
SELECT 'join_use_nulls widens the lambda body, right', countIf(explain LIKE '%column_name: v, result_type: Int16%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM r1 RIGHT JOIN r2 USING (a)
    PREWHERE (arrayMap(z -> v, [1])[1]) != 0 SETTINGS join_use_nulls = 1);
SELECT 'join_use_nulls widens the lambda body, right direct oracle', countIf(explain LIKE '%column_name: v, result_type: Int16%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM r1 RIGHT JOIN r2 USING (a)
    PREWHERE v != 0 SETTINGS join_use_nulls = 1);
SELECT 'join_use_nulls widens the lambda body, right value';
SELECT count() FROM r1 RIGHT JOIN r2 USING (a)
PREWHERE (arrayMap(z -> v, [1])[1]) != 0 SETTINGS join_use_nulls = 1;
SELECT 'join_use_nulls widens the lambda body, full value';
SELECT count() FROM r1 FULL JOIN r2 USING (a)
PREWHERE (arrayMap(z -> v, [1])[1]) != 0 SETTINGS join_use_nulls = 1;
SELECT 'join_use_nulls with ON, bare column';
SELECT count() FROM r1 LEFT JOIN r2 ON r1.a = r2.a
PREWHERE (arrayMap(z -> v, [1])[1]) != 0 SETTINGS join_use_nulls = 1;
SELECT 'lambda argument is not replaced';
SELECT count() FROM r1 INNER JOIN r2 USING (a) PREWHERE (arrayMap(z -> z + v, [1])[1]) != 0;
SELECT 'lambda argument beside a matcher';
SELECT count() FROM r1 INNER JOIN r2 USING (a)
PREWHERE ((arrayMap(z -> tuple(z, * EXCEPT w), [1])[1]).3) != 0;

DROP TABLE IF EXISTS s1;
CREATE TABLE s1 (`k` UInt32, `p` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
DROP TABLE IF EXISTS s2;
CREATE TABLE s2 (`k` UInt64, `q` UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO s1 VALUES (1, 10);
INSERT INTO s2 VALUES (1, 11);

-- A subquery in PREWHERE keeps its own USING supertype: the flag is read from the nearest query
-- scope, which for the inner matcher is the subquery, not the outer PREWHERE.
SELECT 'subquery in PREWHERE keeps its own supertype';
SELECT (SELECT toTypeName(arrayMap(z -> tuple(* EXCEPT q), [1])) FROM s1 INNER JOIN s2 USING (k))
FROM r1 INNER JOIN r2 USING (a) PREWHERE v != 0;
SELECT 'subquery inside a lambda is not visited';
SELECT count() FROM r1 INNER JOIN r2 USING (a) PREWHERE (arrayMap(z -> (SELECT max(p) FROM s1), [1])[1]) != 0;

DROP TABLE s1;
DROP TABLE s2;
DROP TABLE r1;
DROP TABLE r2;

-- 3. Wrapper carriers, including the original CI signature and the inverted mismatch.

DROP TABLE IF EXISTS lc1;
DROP TABLE IF EXISTS lc2;
CREATE TABLE lc1 (`a` LowCardinality(String), `v` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE lc2 (`a` String, `w` UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO lc1 VALUES ('1', 10);
INSERT INTO lc2 VALUES ('1', 11);

SELECT 'LowCardinality(String) against String';
SELECT w, a, v FROM lc1 INNER JOIN lc2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).1) != '';
SELECT 'LowCardinality lambda expansion equals direct expansion', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(LowCardinality(String), Int16, LowCardinality(String))%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT w, a, v FROM lc1 INNER JOIN lc2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).1) != '');
SELECT 'LowCardinality direct expansion', countIf(explain LIKE '%function_name: tuple, function_type: ordinary, result_type: Tuple(LowCardinality(String), Int16, LowCardinality(String))%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT w, a, v FROM lc1 INNER JOIN lc2 USING (a) PREWHERE ((tuple(* EXCEPT w)).1) != '');

DROP TABLE lc1;
DROP TABLE lc2;

DROP TABLE IF EXISTS lcn1;
DROP TABLE IF EXISTS lcn2;
CREATE TABLE lcn1 (`a` LowCardinality(Nullable(String)), `v` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE lcn2 (`a` String, `w` UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO lcn1 VALUES ('1', 10);
INSERT INTO lcn2 VALUES ('1', 11);
SELECT 'LowCardinality(Nullable(String)) against String';
SELECT w, a, v FROM lcn1 INNER JOIN lcn2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;
DROP TABLE lcn1;
DROP TABLE lcn2;

DROP TABLE IF EXISTS inv1;
DROP TABLE IF EXISTS inv2;
CREATE TABLE inv1 (`a` String, `v` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE inv2 (`a` Nullable(String), `w` UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO inv1 VALUES ('1', 10);
INSERT INTO inv2 VALUES ('1', 11);
-- Here the supertype is WIDER than the left column, so the mismatch is inverted.
SELECT 'String against Nullable(String)';
SELECT w, a, v FROM inv1 INNER JOIN inv2 USING (a) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).2) != 0;
DROP TABLE inv1;
DROP TABLE inv2;

-- 6. Multi key USING.

DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;
CREATE TABLE m1 (`a` UInt32, `b` UInt32, `v` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE m2 (`a` UInt64, `b` UInt64, `w` UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO m1 VALUES (1, 2, 10);
INSERT INTO m2 VALUES (1, 2, 11);
SELECT 'multi key USING';
SELECT w, a, b, v FROM m1 INNER JOIN m2 USING (a, b) PREWHERE ((arrayMap(z -> tuple(* EXCEPT w), [1])[1]).3) != 0;
DROP TABLE m1;
DROP TABLE m2;

-- 9. A CROSS JOIN and a join-free query leave the replacement map empty, so the wider descent must
-- be a no-op there.

DROP TABLE IF EXISTS n1;
CREATE TABLE n1 (`a` UInt32, `v` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO n1 VALUES (1, 10);
SELECT 'no join';
SELECT a, v FROM n1 PREWHERE ((arrayMap(z -> tuple(*), [1])[1]).2) != 0;

DROP TABLE IF EXISTS c1;
CREATE TABLE c1 (`id` Int8, `z` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO c1 VALUES (1, 3);
SELECT 'cross join';
SELECT count() FROM n1 CROSS JOIN c1 PREWHERE ((arrayMap(w -> tuple(n1.*), [1])[1]).2) != 0;

-- Three tables, a USING join beside a CROSS JOIN: the qualified matcher path is untouched, and an
-- unqualified matcher there keeps expanding to each table's own columns.
DROP TABLE IF EXISTS n2;
CREATE TABLE n2 (`a` UInt64, `y` Int16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO n2 VALUES (1, 2);
SELECT 'three tables qualified matcher';
SELECT toTypeName(arrayMap(w -> tuple(c1.*), [1])) FROM n1 INNER JOIN n2 USING (a) CROSS JOIN c1
PREWHERE c1.z != 0;
SELECT 'three tables unqualified matcher';
SELECT toTypeName(arrayMap(w -> tuple(* EXCEPT (v, y, z)), [1])) FROM n1 INNER JOIN n2 USING (a) CROSS JOIN c1
PREWHERE c1.z != 0;
SELECT 'three tables unqualified matcher direct oracle';
SELECT toTypeName(tuple(* EXCEPT (v, y, z))) FROM n1 INNER JOIN n2 USING (a) CROSS JOIN c1
PREWHERE c1.z != 0;

DROP TABLE n1;
DROP TABLE n2;
DROP TABLE c1;
