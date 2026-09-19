-- https://github.com/ClickHouse/ClickHouse/issues/100422
-- convert_query_to_cnf distributes `exists(q) OR (a AND b AND c)` into
-- `(exists(q) OR a) AND (exists(q) OR b) AND (exists(q) OR c)`, cloning the correlated
-- exists() subquery into each conjunct. The clones share one action node name, so each
-- must be decorrelated into a single join column. Registering them more than once produced
-- duplicate same-named columns and a LOGICAL_ERROR in HashJoin::getNonJoinedBlocks.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET convert_query_to_cnf = 1;

DROP TABLE IF EXISTS t_04490;
CREATE TABLE t_04490 (i Int32, dt DateTime('UTC')) ENGINE = Memory;
INSERT INTO t_04490 VALUES (1, '2024-01-01 00:00:00'), (3, '2024-06-15 12:00:00'), (5, '2025-01-01 00:00:00');

-- exists() is always true here, so every row matches. The point is that it must not crash
-- and must return the same rows with convert_query_to_cnf on as off.
SELECT i FROM t_04490 WHERE exists((SELECT dt)) OR (dt > 1 AND dt < 100 AND dt != 7) ORDER BY i;

-- The exists() clone count grows with the number of AND terms; check a longer chain too.
SELECT i FROM t_04490 WHERE exists((SELECT dt)) OR (i > 0 AND i < 100 AND i != 7 AND dt != 0) ORDER BY i;

-- Same shape with an extra IN-subquery branch, mirroring the original fuzzer report.
SELECT count() FROM t_04490
WHERE exists((SELECT dt)) OR dt IN (SELECT 1) OR (dt > 1 AND dt < 100 AND dt != 7);

DROP TABLE t_04490;

-- Reduced reproducer from issue #100422 (a correlated exists plus several OR branches, one an AND chain).
DROP TABLE IF EXISTS m_04490;
CREATE TABLE m_04490 (a UInt32) ENGINE = Memory;
INSERT INTO m_04490 VALUES (0), (1), (2);
SELECT * FROM m_04490
WHERE exists((SELECT a <= 100)) OR (a >= 0 AND a <= 50 AND a > 10) OR (2 != a) OR (a = 99)
ORDER BY a;

DROP TABLE m_04490;

-- The dedup keys on the subquery body, not just the action node name, so two DIFFERENT
-- correlated exists() subqueries in one filter must stay distinct (each decorrelates into its
-- own join) and must not be merged. Each is combined with an AND-chain so CNF clones it.
-- The non-subquery OR branch is false for every row here, so the exists() results alone decide
-- the output: merging two distinct bodies into one would change the row set (see per-query notes).
DROP TABLE IF EXISTS d_04490;
DROP TABLE IF EXISTS ds_04490;
CREATE TABLE d_04490 (a Int32, b Int32) ENGINE = Memory;
INSERT INTO d_04490 VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE ds_04490 (x Int32) ENGINE = Memory;
INSERT INTO ds_04490 VALUES (2), (3);

-- existsA (x=a) is true for a in {2,3}; existsB (x=a+1) for a in {1,2}. The OR branch is false for all
-- rows, so this is existsA AND existsB = {2}. A wrong same-name merge (reusing one body for both) would
-- give existsA AND existsA = {2,3} or existsB AND existsB = {1,2}; either differs from {2}.
SELECT a FROM d_04490
WHERE (exists((SELECT 1 FROM ds_04490 WHERE x = a)) OR (b > 1000 AND b < 2000))
  AND (exists((SELECT 1 FROM ds_04490 WHERE x = a + 1)) OR (b > 1000 AND b < 2000))
ORDER BY a;

-- Same two distinct bodies OR'd together, plus a false-for-all AND-chain branch: existsA OR existsB =
-- {1,2,3}. A wrong merge (both bodies -> existsA) would give existsA OR existsA = {2,3}, dropping row 1.
SELECT a FROM d_04490
WHERE exists((SELECT 1 FROM ds_04490 WHERE x = a)) OR exists((SELECT 1 FROM ds_04490 WHERE x = a + 1)) OR (b > 1000 AND b < 2000)
ORDER BY a;

DROP TABLE d_04490;
DROP TABLE ds_04490;
