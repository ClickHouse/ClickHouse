-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105827
-- Fixed by https://github.com/ClickHouse/ClickHouse/pull/101504
--
-- LEFT JOIN ... ON FALSE must produce NULL-extended rows for the right side.
-- A WHERE predicate referencing the right side then evaluates to NULL under
-- SQL three-valued logic and must filter every row out. Before the fix, the
-- join reorder optimizer pushed the WHERE predicate into the LEFT JOIN's ON
-- clause, which only affects matching (not filtering) on the preserved side,
-- so rows leaked through and COUNT(*) returned 5 instead of 0.

DROP TABLE IF EXISTS users_105827;
DROP TABLE IF EXISTS posts_105827;
DROP TABLE IF EXISTS comments_105827;

CREATE TABLE users_105827
(
    id UInt32,
    username Nullable(String),
    email Nullable(String),
    age Nullable(UInt8),
    status Nullable(String),
    created_at DateTime,
    score Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE posts_105827
(
    id UInt32,
    user_id UInt32,
    title Nullable(String),
    content Nullable(String),
    views Nullable(UInt32),
    likes Nullable(UInt32),
    created_at DateTime,
    rating Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE comments_105827
(
    id UInt32,
    post_id UInt32,
    user_id UInt32,
    content Nullable(String),
    is_spam UInt8,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO users_105827 VALUES
    (1, 'alice', 'alice@test.com', 20,   'active',   '2022-01-01 10:00:00', 88.5),
    (2, 'bob',   'bob@test.com',   30,   'active',   '2022-01-02 11:00:00', 92.3),
    (3, 'carol', NULL,             NULL, 'banned',   '2022-01-03 12:00:00', NULL),
    (4, 'dave',  'dave@test.com',  45,   'active',   '2022-01-04 13:00:00', 65.2),
    (5, NULL,    'null@test.com',  18,   'inactive', '2022-01-05 14:00:00', 70.0);

INSERT INTO posts_105827 VALUES
    (1, 1, 'Hello World',  'First post', 100,  10, '2022-01-10 10:00:00', 4.5),
    (2, 1, 'Another Post', NULL,         150,  20, '2022-01-11 11:00:00', 3.0),
    (3, 2, 'Bob Post',     'Content',    NULL,  5, '2022-01-12 12:00:00', NULL),
    (4, 3, NULL,           'Empty',      50,    2, '2022-01-13 13:00:00', 5.0),
    (5, 4, 'Last Post',    'Last',       300,  30, '2022-01-14 14:00:00', 4.9);

INSERT INTO comments_105827 VALUES
    (1, 1, 2, 'Nice post', 0, '2022-01-20 10:00:00'),
    (2, 1, 3, 'Spam here', 1, '2022-01-21 11:00:00'),
    (3, 2, 1, 'Thanks',    0, '2022-01-22 12:00:00'),
    (4, 4, 5, NULL,        0, '2022-01-23 13:00:00');

-- Original query from the issue. Expected: 0
SELECT COUNT(*)
FROM users_105827 AS ref_0
LEFT JOIN
(
    SELECT DISTINCT
        ref_1.post_id AS c0,
        ref_1.is_spam AS c1
    FROM comments_105827 AS ref_1
    WHERE ref_1.is_spam >= ref_1.post_id
) AS subq_0
    ON (ref_0.id = subq_0.c0)
LEFT JOIN posts_105827 AS ref_2
    ON FALSE
WHERE (ref_2.likes >= ref_0.age);

-- NOREC reference shape from the issue. Expected: 0
SELECT SUM(CASE WHEN (ref_2.likes >= ref_0.age) THEN 1 ELSE 0 END + 0)
FROM users_105827 AS ref_0
LEFT JOIN
(
    SELECT DISTINCT
        ref_1.post_id AS c0,
        ref_1.is_spam AS c1
    FROM comments_105827 AS ref_1
    WHERE ref_1.is_spam >= ref_1.post_id
) AS subq_0
    ON (ref_0.id = subq_0.c0)
LEFT JOIN posts_105827 AS ref_2
    ON FALSE;

-- Same as the original query but with the split-filter optimization disabled,
-- which exercises a different join-reorder code path. Expected: 0
SELECT COUNT(*)
FROM users_105827 AS ref_0
LEFT JOIN
(
    SELECT DISTINCT
        ref_1.post_id AS c0,
        ref_1.is_spam AS c1
    FROM comments_105827 AS ref_1
    WHERE ref_1.is_spam >= ref_1.post_id
) AS subq_0
    ON (ref_0.id = subq_0.c0)
LEFT JOIN posts_105827 AS ref_2
    ON FALSE
WHERE (ref_2.likes >= ref_0.age)
SETTINGS query_plan_split_filter = 0;

DROP TABLE users_105827;
DROP TABLE posts_105827;
DROP TABLE comments_105827;
