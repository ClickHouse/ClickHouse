-- Tags: long

-- A qualified matcher (`t.*`) over a JOIN USING key must keep the matched column's own
-- type, not the merged key's. Otherwise it can wrongly become Nullable (join_use_nulls = 0,
-- outer JOIN against a Nullable key) and an aggregate over it throws the exception
-- "Bad cast from type DB::IColumn const* to DB::ColumnNullable const*".

DROP TABLE IF EXISTS t_jn1;
DROP TABLE IF EXISTS t_jn2;
DROP TABLE IF EXISTS t_jn3;

CREATE TABLE t_jn1 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_jn2 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_jn3 (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;

INSERT INTO t_jn1 VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO t_jn2 VALUES (0, 'd'), (1, 'e'), (3, 'f');
INSERT INTO t_jn3 VALUES (0, 'g'), (2, 'h'), (4, 'i');

SET enable_analyzer = 1;

-- The aggregation over the qualified matcher used to throw a Bad cast exception.
SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(t2.*))
    FROM t_jn1 LEFT JOIN t_jn2 AS t2 USING (id) GLOBAL RIGHT JOIN t_jn3 USING (id)
)
SETTINGS join_use_nulls = 0;

-- With join_use_nulls = 0 the qualified-matcher column must have the same type as the
-- explicit reference: the join does not make the data Nullable, so neither must the type.
SELECT toTypeName(sipHash64(t2.*)) = toTypeName(sipHash64(t2.id, t2.value))
FROM t_jn1 LEFT JOIN t_jn2 AS t2 USING (id) RIGHT JOIN t_jn3 USING (id)
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT toTypeName(t2.id)
FROM t_jn1 LEFT JOIN t_jn2 AS t2 USING (id) RIGHT JOIN t_jn3 USING (id)
LIMIT 1
SETTINGS join_use_nulls = 0;

-- The qualified matcher still adopts the common supertype of the USING key, and still
-- becomes Nullable when the join does make the data Nullable.
SELECT toTypeName(t1.a)
FROM (SELECT 1 :: Int32 AS a) AS t1 INNER JOIN (SELECT 1 :: UInt32 AS a) AS t2 USING (a)
SETTINGS join_use_nulls = 0;

SELECT toTypeName(t2.a)
FROM (SELECT 1 :: Int32 AS a) AS t1 LEFT JOIN (SELECT 2 :: UInt32 AS a) AS t2 USING (a)
SETTINGS join_use_nulls = 1;

SELECT toTypeName(t1.a)
FROM (SELECT 1 :: Int32 AS a) AS t1 RIGHT JOIN (SELECT 2 :: UInt32 AS a) AS t2 USING (a)
SETTINGS join_use_nulls = 0;

-- USING can widen the value type along axes other than nullability (e.g. UInt8 -> Int64 or
-- Decimal(9,2) -> Decimal(18,4)). The qualified matcher must take that supertype for the
-- value, exactly like the explicit reference does, so these equalities must hold.
SELECT toTypeName(sipHash64(t1.*)) = toTypeName(sipHash64(t1.a))
FROM (SELECT 1 :: UInt8 AS a) AS t1 LEFT JOIN (SELECT 1 :: Int64 AS a) AS t2 USING (a)
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT toTypeName(sipHash64(t1.*)) = toTypeName(sipHash64(t1.a))
FROM (SELECT 1 :: UInt8 AS a) AS t1 RIGHT JOIN (SELECT 1 :: Int64 AS a) AS t2 USING (a)
LIMIT 1
SETTINGS join_use_nulls = 1;

SELECT toTypeName(sipHash64(t1.*)) = toTypeName(sipHash64(t1.a))
FROM (SELECT 1 :: Decimal(9, 2) AS a) AS t1 LEFT JOIN (SELECT 1 :: Decimal(18, 4) AS a) AS t2 USING (a)
LIMIT 1
SETTINGS join_use_nulls = 0;

-- Aggregating over a qualified matcher whose USING key both widens (UInt8 -> Int64) and
-- gains nullability on the other side must not throw the Bad cast exception with join_use_nulls = 0.
SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(t1.*))
    FROM (SELECT 1 :: UInt8 AS a, 'x' AS v) AS t1 GLOBAL RIGHT JOIN (SELECT 1 :: Int64 AS a) AS t2 USING (a)
)
SETTINGS join_use_nulls = 0;

-- The qualified-matcher column belongs to the inner JOIN it participates in, not the outer
-- USING key. When a sibling USING table is Nullable, the inner-merged key (and the runtime
-- column) is Nullable, so the matcher type must be Nullable too, matching the explicit
-- reference. The matcher used to wrongly take the matched column's own (non-Nullable) type,
-- and an `-OrNull` aggregate over it threw the opposite Bad cast exception
-- "from type DB::ColumnNullable to DB::ColumnVector<unsigned long>".
DROP TABLE IF EXISTS t_jn1_nullable;
DROP TABLE IF EXISTS t_jn3_nullable;
CREATE TABLE t_jn1_nullable (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
CREATE TABLE t_jn3_nullable (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_jn1_nullable VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO t_jn3_nullable VALUES (0, 'g'), (2, 'h'), (4, 'i');

SELECT toTypeName(sipHash64(t2.*)) = toTypeName(sipHash64(t2.id, t2.value))
FROM t_jn1_nullable LEFT JOIN t_jn2 AS t2 USING (id) RIGHT JOIN t_jn3_nullable USING (id)
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT toTypeName(t2.id)
FROM t_jn1_nullable LEFT JOIN t_jn2 AS t2 USING (id) RIGHT JOIN t_jn3_nullable USING (id)
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT count() FROM
(
    SELECT anyHeavyOrNull(sipHash64(t2.*))
    FROM t_jn1_nullable LEFT JOIN t_jn2 AS t2 USING (id) GLOBAL RIGHT JOIN t_jn3_nullable USING (id)
)
SETTINGS join_use_nulls = 0;

-- The USING join `t2` participates in can sit below a non-USING top join (PASTE/CROSS/comma
-- join, or an outer ON join). The qualified matcher must then still adopt the type of the
-- explicit reference, since inspecting only the top node would skip the type correction and
-- leave the matched column with its non-Nullable table type while the runtime column is
-- Nullable, throwing a Bad cast exception from an aggregate over it. The type equality must hold
-- and the aggregate must not throw the exception for every wrapping join shape.

-- PASTE JOIN wrapping the USING join.
SELECT toTypeName(sipHash64(t2.*)) = toTypeName(sipHash64(t2.id, t2.value))
FROM t_jn2 AS t2 RIGHT JOIN t_jn3_nullable USING (id) PASTE JOIN numbers(2) AS n
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(t2.*))
    FROM t_jn2 AS t2 RIGHT JOIN t_jn3_nullable USING (id) PASTE JOIN numbers(2) AS n
)
SETTINGS join_use_nulls = 0;

-- CROSS JOIN wrapping the USING join.
SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(t2.*))
    FROM t_jn2 AS t2 RIGHT JOIN t_jn3_nullable USING (id) CROSS JOIN numbers(2) AS n
)
SETTINGS join_use_nulls = 0;

-- Comma join wrapping the USING join.
SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(t2.*))
    FROM t_jn2 AS t2 RIGHT JOIN t_jn3_nullable USING (id), numbers(2) AS n
)
SETTINGS join_use_nulls = 0;

-- Outer ON join wrapping the USING join.
SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(t2.*))
    FROM t_jn2 AS t2 RIGHT JOIN t_jn3_nullable USING (id) INNER JOIN t_jn1 ON t2.value = t_jn1.value
)
SETTINGS join_use_nulls = 0;

DROP TABLE t_jn1_nullable;
DROP TABLE t_jn3_nullable;

-- Examining every USING join in the tree for a qualified matcher must not pick up a USING join
-- the matched table does not take part in. With `(t_a JOIN t_b USING(id)) CROSS JOIN t_c`,
-- resolving `t_c.*` reaches the inner `USING(id)` only because the key name `id` coincides with
-- `t_c.id`. The matched column must not be retyped against, nor registered as changed-type from,
-- that unrelated key: such a registration rewrites `t_c.id` to `t_a.id` during PREWHERE
-- replacement (and treats a later unqualified `id` as equal to it instead of ambiguous), giving
-- wrong PREWHERE results. The column types here are identical, so the only observable effect is
-- the wrong rewrite; PREWHERE must filter on `t_c.id`, matching the equivalent WHERE.
DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
CREATE TABLE t_a (id Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_b (id UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_c (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_a VALUES (1);
INSERT INTO t_b VALUES (1);
INSERT INTO t_c VALUES (1, 'one'), (2, 'two'), (5, 'five');

-- PREWHERE on the non-participating table's key must filter that table, not the unrelated USING key.
SELECT t_c.* FROM t_a INNER JOIN t_b USING (id) CROSS JOIN t_c PREWHERE t_c.id = 5 SETTINGS join_use_nulls = 0;
SELECT t_c.* FROM t_a INNER JOIN t_b USING (id) CROSS JOIN t_c PREWHERE t_c.id = 1 SETTINGS join_use_nulls = 0;
-- Same with a comma join wrapping the USING join.
SELECT t_c.* FROM t_a INNER JOIN t_b USING (id), t_c PREWHERE t_c.id = 5 SETTINGS join_use_nulls = 0;
-- The rewrite must match the equivalent WHERE (which is not subject to the changed-type replacement).
SELECT t_c.* FROM t_a INNER JOIN t_b USING (id) CROSS JOIN t_c WHERE t_c.id = 5 SETTINGS join_use_nulls = 0;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;

-- A NATURAL JOIN synthesizes its USING key (the common column names) only while resolving the
-- join, after the join-tree walk that counts USING joins has already run. The qualified matcher
-- must still adopt the type the synthesized key gives the explicit reference; otherwise `t1.*`
-- keeps the table (non-Nullable) type while the runtime column is Nullable, and an aggregate over
-- it throws the same Bad cast exception. The type equality must hold and the aggregate must not throw it.
DROP TABLE IF EXISTS nt1;
DROP TABLE IF EXISTS nt2;
CREATE TABLE nt1 (id UInt64, x String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE nt2 (id Nullable(UInt64), y String) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO nt1 VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO nt2 VALUES (0, 'g'), (2, 'h'), (4, 'i');

SELECT toTypeName(sipHash64(nt1.*)) = toTypeName(sipHash64(nt1.id, nt1.x))
FROM nt1 NATURAL RIGHT JOIN nt2
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT toTypeName(nt1.id)
FROM nt1 NATURAL RIGHT JOIN nt2
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT count() FROM
(
    SELECT anyHeavy(sipHash64(nt1.*))
    FROM nt1 NATURAL RIGHT JOIN nt2
)
SETTINGS join_use_nulls = 0;

DROP TABLE nt1;
DROP TABLE nt2;

-- A qualified matcher inside a lambda body (`arrayMap(x -> t.*, ...)`) resolves through a fresh
-- child scope, but still expands `t.*` from the parent query's join tree. The USING-join presence
-- check must look at that parent query scope, not the lambda's (whose counter is zero); otherwise
-- the matcher keeps the table (non-Nullable) type while the explicit reference is Nullable, and an
-- aggregate over it throws the same Bad cast exception.
DROP TABLE IF EXISTS lt2;
DROP TABLE IF EXISTS lt1_nullable;
DROP TABLE IF EXISTS lt3_nullable;
CREATE TABLE lt2 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE lt1_nullable (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
CREATE TABLE lt3_nullable (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO lt2 VALUES (0, 'd'), (1, 'e'), (3, 'f');
INSERT INTO lt1_nullable VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO lt3_nullable VALUES (0, 'g'), (2, 'h'), (4, 'i');

SELECT toTypeName(arrayMap(x -> sipHash64(t2.*), [1])) = toTypeName(arrayMap(x -> sipHash64(t2.id, t2.value), [1]))
FROM lt1_nullable LEFT JOIN lt2 AS t2 USING (id) RIGHT JOIN lt3_nullable USING (id)
LIMIT 1
SETTINGS join_use_nulls = 0;

SELECT count() FROM
(
    SELECT anyHeavyOrNull(arrayMap(x -> sipHash64(t2.*), [1]))
    FROM lt1_nullable LEFT JOIN lt2 AS t2 USING (id) GLOBAL RIGHT JOIN lt3_nullable USING (id)
)
SETTINGS join_use_nulls = 0;

DROP TABLE lt2;
DROP TABLE lt1_nullable;
DROP TABLE lt3_nullable;

DROP TABLE t_jn1;
DROP TABLE t_jn2;
DROP TABLE t_jn3;
