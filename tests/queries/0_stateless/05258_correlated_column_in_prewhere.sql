-- A PREWHERE of a correlated subquery that references a column of the outer query: the condition must be applied,
-- and the outer column must not be read from the inner table.

DROP TABLE IF EXISTS t_prewhere_outer;
DROP TABLE IF EXISTS t_prewhere_outer_final;
DROP TABLE IF EXISTS t_prewhere_outer_clash;
DROP TABLE IF EXISTS t_prewhere_outer_ab;

CREATE TABLE t_prewhere_outer (value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_outer VALUES (10), (11);

-- Only a column of the outer query.
SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE t1.value + 1 = 12)
ORDER BY value;

-- Captured by a lambda, also by one whose argument has the same name.
SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE arrayMap(x -> x + t1.value, [1])[1] = 12)
ORDER BY value;

SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE arrayMap(value -> value + t1.value, [toUInt8(1)])[1] = 12)
ORDER BY value;

-- Together with a column of the inner table that has the same name.
SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE arrayMap(x -> x + t2.value, [1])[1] < arrayMap(x -> x + t1.value, [1])[1])
ORDER BY value;

SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE t2.value = t1.value AND t2.value > 10)
ORDER BY value;

SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE t2.value + 1 = t1.value + 1 AND t2.value > 10)
ORDER BY value;

-- A scalar subquery and a `merge` table.
SELECT value, (SELECT sum(t2.value) FROM t_prewhere_outer AS t2 PREWHERE t2.value <= arrayMap(x -> x + t1.value, [0])[1])
FROM t_prewhere_outer AS t1
ORDER BY value;

SELECT value FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM merge(currentDatabase(), '^t_prewhere_outer$') AS t2 PREWHERE arrayMap(x -> x + t1.value, [1])[1] = 12)
ORDER BY value;

-- Refused instead of ignored: a table without PREWHERE support, FINAL, a JOIN, a column named like the generated name of the outer column.
SELECT count() FROM t_prewhere_outer AS t1 WHERE exists(SELECT 1 PREWHERE t1.value = 12); -- { serverError ILLEGAL_PREWHERE }

CREATE TABLE t_prewhere_outer_final (value UInt64) ENGINE = ReplacingMergeTree ORDER BY value;
INSERT INTO t_prewhere_outer_final VALUES (10);
SELECT count() FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer_final AS t2 FINAL PREWHERE arrayMap(x -> x + t1.value, [1])[1] = 12); -- { serverError ILLEGAL_PREWHERE }

SELECT count() FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer AS t2 INNER JOIN t_prewhere_outer AS t3 ON t2.value = t3.value PREWHERE t1.value + 1 = 12); -- { serverError NOT_IMPLEMENTED }

CREATE TABLE t_prewhere_outer_clash (`__table1.value` UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_outer_clash VALUES (1);
SELECT count() FROM t_prewhere_outer AS t1
WHERE exists(SELECT 1 FROM t_prewhere_outer_clash AS t2 PREWHERE t1.value + 1 = 12); -- { serverError NOT_IMPLEMENTED }

-- The same with the outer column referenced only in another UNION arm, and the query without that column.
CREATE TABLE t_prewhere_outer_ab (a UInt64, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_outer_ab VALUES (10, 20), (11, 21);
SELECT count() FROM t_prewhere_outer_ab AS t1
WHERE exists((SELECT 1 FROM t_prewhere_outer AS t2 PREWHERE t1.a = 10 AND t2.value = 10)
    UNION ALL (SELECT 1 FROM t_prewhere_outer AS t3 WHERE t1.value = 999));
SELECT count() FROM t_prewhere_outer_ab AS t1
WHERE exists((SELECT 1 FROM t_prewhere_outer_clash AS t2 PREWHERE t1.a = 10 AND t2.`__table1.value` = 1)
    UNION ALL (SELECT 1 FROM t_prewhere_outer AS t3 WHERE t1.value = 999)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_prewhere_outer_ab;
DROP TABLE t_prewhere_outer_clash;
DROP TABLE t_prewhere_outer_final;
DROP TABLE t_prewhere_outer;
