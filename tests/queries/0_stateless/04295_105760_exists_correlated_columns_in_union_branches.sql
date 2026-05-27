-- https://github.com/ClickHouse/ClickHouse/issues/105760
--
-- UNION variant of the EXISTS-correlated-projection bug. The fix in #105900
-- strips the inner projection of a correlated EXISTS subquery to `SELECT 1`.
-- For `EXISTS ((SELECT ...) UNION [ALL|DISTINCT] (SELECT ...))` the same
-- stripping must be applied to each branch, otherwise projection-only
-- references to outer-correlated columns leak into the wrapper's correlated
-- columns and `buildLogicalJoin` turns them into equality predicates that
-- drop outer rows whose `Nullable` columns hold `NULL`.
--
-- `INTERSECT` and `EXCEPT` branch semantics depend on row values across
-- branches, so the fix must NOT strip projections there.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS users_105760_union;
DROP TABLE IF EXISTS posts_105760_union;

CREATE TABLE users_105760_union
(
    id UInt32,
    username Nullable(String),
    age Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE posts_105760_union
(
    id UInt32,
    user_id UInt32,
    likes Nullable(UInt32),
    rating Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO users_105760_union VALUES
    (1, 'alice', 20),
    (2, 'bob',   30),
    (3, 'carol', NULL),
    (4, 'dave',  45),
    (5, NULL,    18);

INSERT INTO posts_105760_union VALUES
    (1, 1, 10, 4.5),
    (2, 1, 20, 3.0),
    (3, 2,  5, NULL),
    (4, 3,  2, 5.0),
    (5, 4, 30, 4.9);

-- UNION ALL of two branches that both reference outer-correlated columns only in
-- their projections. The subquery returns rows for every outer row, so EXISTS
-- must be true and the outer count is 5 (= rows of posts_105760_union with
-- likes != user_id).
SELECT 'union_all', COUNT(*) FROM
(
    SELECT ref_0.likes AS c1, ref_0.user_id AS c2 FROM posts_105760_union AS ref_0
    WHERE EXISTS (
        (SELECT ref_0.likes FROM users_105760_union AS ref_1 WHERE ref_1.username IS NULL)
        UNION ALL
        (SELECT ref_0.likes FROM users_105760_union AS ref_2 WHERE ref_2.id >= ref_2.age)
    )
) AS subq_0 WHERE subq_0.c2 != subq_0.c1;

-- UNION DISTINCT: same semantics for EXISTS purposes (non-empty iff any branch
-- is non-empty).
SELECT 'union_distinct', COUNT(*) FROM
(
    SELECT ref_0.likes AS c1, ref_0.user_id AS c2 FROM posts_105760_union AS ref_0
    WHERE EXISTS (
        (SELECT ref_0.likes FROM users_105760_union AS ref_1 WHERE ref_1.username IS NULL)
        UNION DISTINCT
        (SELECT ref_0.likes FROM users_105760_union AS ref_2 WHERE ref_2.id >= ref_2.age)
    )
) AS subq_0 WHERE subq_0.c2 != subq_0.c1;

-- UNION ALL where neither branch has a WHERE correlation, only projection
-- correlation. The decorrelation must not introduce `outer.X = inner.X`
-- predicates for these projection-only references.
SELECT 'union_all_projection_only', COUNT(*) FROM
(
    SELECT ref_0.likes AS c1, ref_0.user_id AS c2 FROM posts_105760_union AS ref_0
    WHERE EXISTS (
        (SELECT ref_0.likes FROM users_105760_union AS ref_1 WHERE ref_1.id > 0)
        UNION ALL
        (SELECT ref_0.likes FROM users_105760_union AS ref_2 WHERE ref_2.id > 0)
    )
) AS subq_0 WHERE subq_0.c2 != subq_0.c1;

-- UNION ALL where one branch is correlated in its WHERE clause. The fix
-- preserves WHERE-correlation while removing projection-only correlation.
SELECT 'union_all_where_corr', COUNT(*) FROM
(
    SELECT ref_0.user_id AS c1 FROM posts_105760_union AS ref_0
    WHERE EXISTS (
        (SELECT ref_0.likes FROM users_105760_union AS ref_1 WHERE ref_1.id = ref_0.user_id)
        UNION ALL
        (SELECT ref_0.likes FROM users_105760_union AS ref_2 WHERE ref_2.id = ref_0.user_id)
    )
) AS subq_0;

-- INTERSECT must NOT be stripped: result depends on row VALUE overlap, not
-- just non-emptiness. Branch 1 returns {1,2,3,4,5}, branch 2 returns {1,2},
-- so intersection is {1,2} (non-empty) and EXISTS is true for every outer row.
SELECT 'intersect_overlap', COUNT(*) FROM posts_105760_union AS ref_0
WHERE EXISTS (
    (SELECT id FROM users_105760_union WHERE id >= 1)
    INTERSECT
    (SELECT id FROM users_105760_union WHERE id <= 2)
);

-- EXCEPT must NOT be stripped: branch 1 minus branch 2. Branch 1 returns
-- {1,2,3,4,5}, branch 2 returns {1}, so EXCEPT is {2,3,4,5} (non-empty).
SELECT 'except_remaining', COUNT(*) FROM posts_105760_union AS ref_0
WHERE EXISTS (
    (SELECT id FROM users_105760_union WHERE id >= 1)
    EXCEPT
    (SELECT id FROM users_105760_union WHERE id = 1)
);

-- EXCEPT where the branches are equal must produce an empty set, so EXISTS
-- is false. If the fix incorrectly stripped projections, branch 2 would
-- become `SELECT 1` and the result could differ.
SELECT 'except_all_removed', COUNT(*) FROM posts_105760_union AS ref_0
WHERE EXISTS (
    (SELECT id FROM users_105760_union WHERE id >= 1)
    EXCEPT
    (SELECT id FROM users_105760_union WHERE id >= 1)
);

DROP TABLE users_105760_union;
DROP TABLE posts_105760_union;
