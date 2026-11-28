-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105760
-- A correlated EXISTS / scalar subquery whose correlation lives only in the projection, combined
-- with a non-correlated row-count-affecting clause, reduces the inner relation to a zero-column
-- body. Such a relation used to lose its row count on the streamed side of the decorrelation join,
-- so EXISTS became false for every outer row (and a correlated scalar returned NULL).

-- Correlated subqueries require the analyzer; force it so the test also runs under the old-analyzer CI config.
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS users_04490;
CREATE TABLE users_04490 (uid Int32, name String, age Int32) ENGINE = Memory;
INSERT INTO users_04490 VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

-- EXISTS: correlation only in the projection, non-correlated inner WHERE. Must return 3.
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT t.name FROM users_04490 WHERE age != uid);

-- The result must not depend on the physical join build side / decorrelation join kind.
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT t.name FROM users_04490 WHERE age != uid) SETTINGS query_plan_join_swap_table = false;
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT t.name FROM users_04490 WHERE age != uid) SETTINGS query_plan_join_swap_table = true;
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT t.name FROM users_04490 WHERE age != uid) SETTINGS correlated_subqueries_default_join_kind = 'left';
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT t.name FROM users_04490 WHERE age != uid) SETTINGS correlated_subqueries_default_join_kind = 'right';

-- Correlated scalar with implicit aggregation hits the same zero-column inner body.
-- max(u.uid) over a non-empty set is u.uid, so `m` must equal `uid`.
SELECT u.uid, (SELECT max(u.uid) FROM users_04490 AS v WHERE v.age > 40) AS m FROM users_04490 AS u ORDER BY u.uid;

-- Must-not-regress: real WHERE-correlation.
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT 1 FROM users_04490 AS v WHERE v.age > 40 AND v.uid != t.uid);
-- Must-not-regress: non-correlated EXISTS.
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT 1 FROM users_04490 AS v WHERE v.age > 40);
-- Must-not-regress: empty inner relation -> EXISTS is false for all rows.
SELECT count() FROM users_04490 AS t WHERE EXISTS (SELECT t.name FROM users_04490 AS v WHERE v.age > 1000);

-- Correlated UNION with mixed arms: one arm's body is zero-column (correlation only in the
-- projection), the other projects a real inner column. The row marker must stay internal to its
-- arm; otherwise the arms get mismatched widths (UNION_ALL_RESULT_STRUCTURES_MISMATCH). Must return 3.
SELECT count() FROM users_04490 AS t WHERE EXISTS (
    SELECT t.name FROM users_04490 WHERE age != uid
    UNION ALL
    SELECT name FROM users_04490 WHERE age != uid
);
-- Reversed arm order (non-zero-column arm first).
SELECT count() FROM users_04490 AS t WHERE EXISTS (
    SELECT name FROM users_04490 WHERE age != uid
    UNION ALL
    SELECT t.name FROM users_04490 WHERE age != uid
);
-- Both arms zero-column (both correlated in the projection only, same correlated column).
SELECT count() FROM users_04490 AS t WHERE EXISTS (
    SELECT t.name FROM users_04490 WHERE age != uid
    UNION ALL
    SELECT t.name FROM users_04490 WHERE uid != age
);

DROP TABLE users_04490;
