-- https://github.com/ClickHouse/ClickHouse/issues/105760
--
-- `EXISTS (subquery)` must only check whether the subquery returns at least one row.
-- The contents of the subquery's SELECT list must not affect query semantics,
-- so references to outer-correlated columns in the projection must produce the
-- same result as `EXISTS (SELECT 1 ...)`.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS users_105760;
DROP TABLE IF EXISTS posts_105760;

CREATE TABLE users_105760
(
    id UInt32,
    username Nullable(String),
    age Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE posts_105760
(
    id UInt32,
    user_id UInt32,
    likes Nullable(UInt32),
    rating Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO users_105760 VALUES
    (1, 'alice', 20),
    (2, 'bob',   30),
    (3, 'carol', NULL),
    (4, 'dave',  45),
    (5, NULL,    18);

INSERT INTO posts_105760 VALUES
    (1, 1, 10, 4.5),
    (2, 1, 20, 3.0),
    (3, 2,  5, NULL),
    (4, 3,  2, 5.0),
    (5, 4, 30, 4.9);

-- Constant SELECT list: must match the correlated SELECT list version below.
SELECT 'select 1', COUNT(*) FROM
(
    SELECT ref_0.likes AS c1, ref_0.user_id AS c2
    FROM posts_105760 AS ref_0
    WHERE EXISTS (
        SELECT 1
        FROM users_105760 AS ref_1
        WHERE ref_1.username IS NULL OR ref_1.id >= ref_1.age
    )
) AS subq_0
WHERE subq_0.c2 != subq_0.c1;

-- Correlated columns in the SELECT list of EXISTS: equivalent semantics.
SELECT 'select correlated', COUNT(*) FROM
(
    SELECT ref_0.likes AS c1, ref_0.user_id AS c2
    FROM posts_105760 AS ref_0
    WHERE EXISTS (
        SELECT ref_0.likes AS c0, ref_0.rating AS c1, 44 AS c2, ref_0.user_id AS c3, ref_0.likes AS c4
        FROM users_105760 AS ref_1
        WHERE ref_1.username IS NULL OR ref_1.id >= ref_1.age
    )
) AS subq_0
WHERE subq_0.c2 != subq_0.c1;

-- Outer-only correlation in the projection, no correlation in WHERE.
-- EXISTS must be true for every outer row because the subquery returns rows.
SELECT 'projection only', count() FROM posts_105760 AS ref_0
WHERE EXISTS (
    SELECT ref_0.likes, ref_0.rating, 44
    FROM users_105760 AS ref_1
    WHERE ref_1.id > 0
);

-- Correlation in WHERE only. Must still be evaluated as a correlated EXISTS
-- (one matching row per post.user_id in users_105760.id).
SELECT 'where only', count() FROM posts_105760 AS ref_0
WHERE EXISTS (
    SELECT 1
    FROM users_105760 AS ref_1
    WHERE ref_1.id = ref_0.user_id
);

-- Same as above but with the same projection-only correlations added.
-- Must produce the same result as the WHERE-only case above.
SELECT 'where and projection', count() FROM posts_105760 AS ref_0
WHERE EXISTS (
    SELECT ref_0.likes, ref_0.rating
    FROM users_105760 AS ref_1
    WHERE ref_1.id = ref_0.user_id
);

-- Aggregate in projection. `count()` over an empty input still returns one row,
-- so `EXISTS (SELECT count(...))` must be true for every outer row. The fix
-- must NOT strip the projection in this case (implicit aggregation matters).
SELECT 'aggregate in projection', count() FROM posts_105760 AS ref_0
WHERE EXISTS (
    SELECT count(ref_0.likes)
    FROM users_105760 AS ref_1
    WHERE ref_1.id > 999
);

DROP TABLE users_105760;
DROP TABLE posts_105760;
