DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS e;
DROP TABLE IF EXISTS f;

SET enable_analyzer = 1;
SET join_use_nulls = 0;
SET query_plan_optimize_join_order_limit = 64;
SET query_plan_optimize_join_order_randomize = 1;
SET query_plan_merge_expression_into_join = 1;

CREATE TABLE a (id Int32, x Int32, s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE b (id Int32, x Nullable(Int32), s Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE c (id Int32, x Int32, s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE d (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE f (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO a VALUES (1, 10, 'aa'), (2, 20, 'bb'), (3, 30, 'cc'), (4, 40, 'dd'), (5, 50, 'ee');
INSERT INTO b VALUES (1, 100, 'p'), (2, NULL, 'q'), (3, 300, NULL), (5, 500, 'r'), (6, 600, 's');
INSERT INTO c VALUES (1, 11, 'A'), (2, 22, 'B'), (3, 33, 'C'), (5, 55, 'E'), (6, 66, 'F');
INSERT INTO d VALUES (1, 1), (3, 3), (3, 33), (5, 5), (7, 7);
INSERT INTO e VALUES (1, 1), (3, 3), (5, 5), (8, 8);
INSERT INTO f VALUES (1, 1000), (3, 3000), (5, 5000), (9, 9);


SELECT '-- 1 --';
SELECT u.id, u.k, u.v, d.x AS dx FROM
(
    SELECT t.id AS id, t.k AS k, t.v + c.x AS v FROM
    (
        SELECT a.id AS id, upper(a.s) AS k, a.x + coalesce(b.x, 0) AS v
        FROM a JOIN b ON a.id = b.id WHERE a.x + coalesce(b.x, 0) > 50
    ) t JOIN c ON t.id = c.id
) u JOIN d ON u.id = d.id
ORDER BY ALL;

SELECT '-- 2 --';
SELECT l.id, l.k, r.cx FROM
    (SELECT a.id AS id, upper(a.s) AS k FROM a LEFT JOIN b ON a.id = b.id WHERE coalesce(b.x, 0) >= 0) l
    JOIN (SELECT c.id AS id, c.x AS cx FROM c JOIN d ON c.id = d.id WHERE d.x > 0) r ON l.id = r.id
ORDER BY ALL;

SELECT '-- 3 --';
SELECT u.id, u.w FROM
(
    SELECT t.id AS id, t.v * 10 AS w FROM
    (
        SELECT a.id AS id, a.x + coalesce(b.x, 0) AS v FROM a LEFT JOIN b ON a.id = b.id WHERE a.x < 100
    ) t JOIN c ON t.id = c.id WHERE t.v >= 0
) u JOIN d ON u.id = d.id WHERE u.w > 0
ORDER BY ALL;

SELECT '-- 4 --';
SELECT l.id, l.key, r.key AS rkey FROM
    (SELECT a.id AS id, upper(a.s) AS key FROM a JOIN b ON a.id = b.id) l
    JOIN (SELECT c.id AS id, upper(c.s) AS key FROM c JOIN d ON c.id = d.id) r ON l.id = r.id
ORDER BY ALL;

SELECT '-- 5 --';
SELECT l.id, l.bx, c.x AS cx FROM
    (SELECT a.id AS id, b.x AS bx FROM a LEFT JOIN b ON a.id = b.id WHERE b.x IS NULL OR b.x > 0) l
    JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 6 --';
SELECT l.id, l.v, c.x AS cx FROM
    (SELECT a.id AS id, a.x + coalesce(b.x, 0) AS v FROM a LEFT JOIN b ON a.id = b.id WHERE a.x + coalesce(b.x, 0) > 50) l
    JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 7 --';
SELECT l.id, l.bx, l.bs FROM
    (SELECT a.id AS id, b.x AS bx, b.s AS bs FROM a LEFT JOIN b ON a.id = b.id
     WHERE coalesce(b.x, 0) >= 0 AND (b.s IS NULL OR b.s != 'zzz')) l
    JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 8 --';
SELECT l.id, l.ax, c.x AS cx FROM
    (SELECT b.id AS id, a.x AS ax FROM a RIGHT JOIN b ON a.id = b.id WHERE coalesce(a.x, 0) >= 0) l
    JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 9 --';
SELECT l.id, l.bx, c.x AS cx FROM
    (SELECT a.id AS id, b.x AS bx FROM a LEFT JOIN b ON a.id = b.id WHERE coalesce(b.x, 0) >= 0) l
    LEFT JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 10 --';
SELECT a.id, a.x AS ax, r.cx, r.dx FROM
    a JOIN (SELECT c.id AS id, c.x AS cx, d.x AS dx FROM c JOIN d ON c.id = d.id WHERE d.x < 100) r ON a.id = r.id
ORDER BY ALL;

SELECT '-- 11 --';
SELECT l.id, l.v, c.x AS cx FROM
    (SELECT a.id AS id, a.x * 2 AS v FROM a, b WHERE a.id = b.id AND a.x > 10) l
    JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 12 --';
SELECT l.id, l.bx, c.x AS cx FROM
    (SELECT a.id AS id, b.x AS bx FROM a LEFT JOIN b ON a.id = b.id WHERE b.x IS NULL OR b.x > 0) l
    JOIN c ON l.id = c.id
ORDER BY ALL
SETTINGS join_use_nulls = 1;

SELECT '-- 13 --';
SELECT l.id, l.bx, c.x AS cx FROM
    (SELECT a.id AS id, b.x AS bx FROM a LEFT JOIN b ON a.id = b.id WHERE b.x > 100) l
    JOIN c ON l.id = c.id
ORDER BY ALL;

SELECT '-- 14 --';
SELECT l.id, l.v, c.x AS cx FROM
    (SELECT a.id AS id, b.x + 1 AS v FROM a LEFT JOIN b ON a.id = b.id WHERE b.x + 1 > 101) l
    JOIN c ON l.id = c.id
ORDER BY ALL
SETTINGS join_use_nulls = 1;

SELECT '-- 15 --';
SELECT l.id, l.dx FROM
    (SELECT c.id AS id, d.x AS dx FROM c LEFT JOIN d ON c.id = d.id WHERE d.x > 2) l
    JOIN a ON l.id = a.id
ORDER BY ALL
SETTINGS join_use_nulls = 1;

SELECT '-- 16 --';
SELECT z6.id, z6.acc FROM
(
    SELECT z5.id AS id, z5.acc + f.x AS acc FROM
    (
        SELECT z4.id AS id, z4.acc + e.x AS acc FROM
        (
            SELECT z3.id AS id, z3.acc + d.x AS acc FROM
            (
                SELECT z2.id AS id, z2.acc + c.x AS acc FROM
                (
                    SELECT a.id AS id, a.x + coalesce(b.x, 0) AS acc
                    FROM a LEFT JOIN b ON a.id = b.id WHERE a.x + coalesce(b.x, 0) > 0
                ) z2 JOIN c ON z2.id = c.id WHERE z2.acc > 0
            ) z3 JOIN d ON z3.id = d.id WHERE z3.acc > 0
        ) z4 JOIN e ON z4.id = e.id WHERE z4.acc > 0
    ) z5 JOIN f ON z5.id = f.id WHERE z5.acc > 0
) z6
ORDER BY ALL;

SELECT
    'merge on' AS merge,
    countIf(trimLeft(explain) LIKE 'ReadFromMergeTree%') AS reads,
    countIf(trimLeft(explain) LIKE 'JoinLogical%') AS joins,
    countIf(trimLeft(explain) LIKE 'Filter%') AS boundary_filters
FROM (EXPLAIN PLAN keep_logical_steps = 1, description = 0
    SELECT u.id, u.k, u.v, d.x AS dx FROM
    (
        SELECT t.id AS id, t.k AS k, t.v + c.x AS v FROM
        (
            SELECT a.id AS id, upper(a.s) AS k, a.x + coalesce(b.x, 0) AS v
            FROM a JOIN b ON a.id = b.id WHERE a.x + coalesce(b.x, 0) > 50
        ) t JOIN c ON t.id = c.id
    ) u JOIN d ON u.id = d.id
    SETTINGS query_plan_merge_expression_into_join = 1, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', enable_join_runtime_filters = 0);

SELECT
    'merge off' AS merge,
    countIf(trimLeft(explain) LIKE 'ReadFromMergeTree%') AS reads,
    countIf(trimLeft(explain) LIKE 'JoinLogical%') AS joins,
    countIf(trimLeft(explain) LIKE 'Filter%') AS boundary_filters
FROM (EXPLAIN PLAN keep_logical_steps = 1, description = 0
    SELECT u.id, u.k, u.v, d.x AS dx FROM
    (
        SELECT t.id AS id, t.k AS k, t.v + c.x AS v FROM
        (
            SELECT a.id AS id, upper(a.s) AS k, a.x + coalesce(b.x, 0) AS v
            FROM a JOIN b ON a.id = b.id WHERE a.x + coalesce(b.x, 0) > 50
        ) t JOIN c ON t.id = c.id
    ) u JOIN d ON u.id = d.id
    SETTINGS query_plan_merge_expression_into_join = 0, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_algorithm = 'greedy', enable_join_runtime_filters = 0);

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP TABLE d;
DROP TABLE e;
DROP TABLE f;
