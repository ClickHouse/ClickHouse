-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105815
-- A correlated EXISTS subquery over a RIGHT JOIN with constant ON predicates (OR TRUE / ON FALSE)
-- and a nullable outer reference used to break ternary-logic partitioning: the branches
-- P / NOT P / P IS NULL all counted 0 while COUNT(*) = 4, violating
-- COUNT(P) + COUNT(NOT P) + COUNT(P IS NULL) = COUNT(*).
-- Correlated subqueries require the analyzer, so keep it enabled.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS posts;
DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS orders;

CREATE TABLE posts
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

CREATE TABLE comments
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

CREATE TABLE orders
(
    id UInt32,
    user_id UInt32,
    amount Nullable(Float64),
    status Nullable(String),
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO posts VALUES
(1, 1, 'Hello World',  'First post', 100, 10, '2022-01-10 10:00:00', 4.5),
(2, 1, 'Another Post', NULL,         150, 20, '2022-01-11 11:00:00', 3.0),
(3, 2, 'Bob Post',     'Content',    NULL,  5, '2022-01-12 12:00:00', NULL),
(4, 3, NULL,           'Empty',      50,   2, '2022-01-13 13:00:00', 5.0),
(5, 4, 'Last Post',    'Last',       300, 30, '2022-01-14 14:00:00', 4.9);

INSERT INTO comments VALUES
(1, 1, 2, 'Nice post', 0, '2022-01-20 10:00:00'),
(2, 1, 3, 'Spam here', 1, '2022-01-21 11:00:00'),
(3, 2, 1, 'Thanks',    0, '2022-01-22 12:00:00'),
(4, 4, 5, NULL,        0, '2022-01-23 13:00:00');

INSERT INTO orders VALUES
(1, 1, 100.00, 'paid',    '2022-02-01 09:00:00'),
(2, 1, 200.50, 'shipped', '2022-02-02 10:00:00'),
(3, 2, NULL,   'failed',  '2022-02-03 11:00:00'),
(4, 3, 50.00,  'paid',    '2022-02-04 12:00:00'),
(5, 5, 999.99, 'paid',    '2022-02-05 13:00:00');

-- Baseline row count.
SELECT COUNT(*)
FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0;

-- Lock in the exact ternary-logic partition, not just its sum. The predicate is FALSE
-- for every row because the correlated EXISTS is always TRUE (ON ... OR TRUE keeps rows
-- and the second outer join ON FALSE still preserves one side), so for each outer-join
-- type the branches must be P = 0, NOT P = 4, P IS NULL = 0 (and still sum to COUNT(*) = 4).
-- RIGHT OUTER JOIN.
SELECT
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE (9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 RIGHT JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) RIGHT JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END)
) AS cnt_p,
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE NOT ((9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 RIGHT JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) RIGHT JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END))
) AS cnt_not_p,
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE ((9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 RIGHT JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) RIGHT JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END)) IS NULL
) AS cnt_p_is_null;

-- LEFT OUTER JOIN.
SELECT
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE (9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 LEFT JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) LEFT JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END)
) AS cnt_p,
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE NOT ((9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 LEFT JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) LEFT JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END))
) AS cnt_not_p,
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE ((9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 LEFT JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) LEFT JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END)) IS NULL
) AS cnt_p_is_null;

-- FULL OUTER JOIN.
SELECT
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE (9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 FULL JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) FULL JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END)
) AS cnt_p,
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE NOT ((9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 FULL JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) FULL JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END))
) AS cnt_not_p,
(
    SELECT COUNT(*)
    FROM (SELECT c.created_at c0, c.post_id c1 FROM comments c WHERE TRUE) subq_0
    WHERE ((9.53 < CASE WHEN subq_0.c1 IS NULL THEN CASE WHEN (CASE WHEN subq_0.c1 IS NOT NULL THEN 'z' ELSE 'f' END) >= 'h' THEN 37.78 ELSE 93.53 END ELSE 100.81 END)
      AND TRUE
      AND (CASE WHEN EXISTS (SELECT 1 FROM comments c2 FULL JOIN orders o ON (((o.id <> o.id OR TRUE) OR subq_0.c1 IS NULL)) FULL JOIN posts p4 ON FALSE WHERE TRUE) THEN FALSE ELSE TRUE END)) IS NULL
) AS cnt_p_is_null;

DROP TABLE posts;
DROP TABLE comments;
DROP TABLE orders;
