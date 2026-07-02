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
