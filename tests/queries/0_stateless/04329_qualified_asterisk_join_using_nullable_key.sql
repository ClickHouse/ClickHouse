-- A qualified matcher (`t.*`) over a JOIN USING key must keep the matched column's own
-- type, not the merged key's. Otherwise it can wrongly become Nullable (join_use_nulls = 0,
-- outer JOIN against a Nullable key) and an aggregate over it aborts with
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

-- The aggregation over the qualified matcher used to crash the server.
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
-- gains nullability on the other side must not crash with join_use_nulls = 0.
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
-- and an `-OrNull` aggregate over it aborted with the opposite Bad cast
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

DROP TABLE t_jn1_nullable;
DROP TABLE t_jn3_nullable;

DROP TABLE t_jn1;
DROP TABLE t_jn2;
DROP TABLE t_jn3;
