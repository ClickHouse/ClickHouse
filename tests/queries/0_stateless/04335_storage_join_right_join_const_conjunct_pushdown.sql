SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_left_04335;
DROP TABLE IF EXISTS t_join_04335;

CREATE TABLE t_left_04335 (key2 UInt64, key1 Int64) ENGINE = Memory;
INSERT INTO t_left_04335 VALUES (2, -2), (3, -3);

CREATE TABLE t_join_04335 (key2 UInt64, key1 Int64) ENGINE = Join(ALL, RIGHT, key1, key2);
INSERT INTO t_join_04335 VALUES (2, -2), (6, -6);

-- A constant conjunct must not be pushed to the non-preserved (left) side of a RIGHT JOIN
-- and dropped from the post-join filter. Otherwise a NULL/falsy constant filters out the
-- left input, the join fabricates non-matched rows, and they escape the post-join filter.

SELECT 'bare predicate, QUALIFY NULL';
SELECT * FROM t_left_04335 ALL RIGHT JOIN t_join_04335
    ON t_join_04335.key2 = t_left_04335.key2 AND t_join_04335.key1 = t_left_04335.key1
WHERE (t_left_04335.key2 <=> t_join_04335.key2) = t_left_04335.key2
QUALIFY NULL;

SELECT 'no-op AND constant, QUALIFY NULL';
SELECT * FROM t_left_04335 ALL RIGHT JOIN t_join_04335
    ON t_join_04335.key2 = t_left_04335.key2 AND t_join_04335.key1 = t_left_04335.key1
WHERE and((t_left_04335.key2 <=> t_join_04335.key2) = t_left_04335.key2, 1)
QUALIFY NULL;

SELECT 'baseline, no WHERE keeps both right rows';
SELECT * FROM t_left_04335 ALL RIGHT JOIN t_join_04335
    ON t_join_04335.key2 = t_left_04335.key2 AND t_join_04335.key1 = t_left_04335.key1
ORDER BY t_join_04335.key2;

DROP TABLE t_left_04335;
DROP TABLE t_join_04335;
