-- An outer JOIN becomes INNER when a WHERE conjunct rejects the not-matched rows, also if the WHERE has an IN (subquery).
-- Random settings limits: query_plan_convert_outer_join_to_inner_join=(1, None)

DROP TABLE IF EXISTS t1_05320;
DROP TABLE IF EXISTS t2_05320;
DROP TABLE IF EXISTS t3_05320;
DROP TABLE IF EXISTS t4_05320;

CREATE TABLE t1_05320 (c1 Int64, c2 Int64, c3 DateTime64(9), c4 DateTime64(9)) ENGINE = MergeTree ORDER BY c1;
CREATE TABLE t2_05320 (c1 Int64, c2 LowCardinality(String)) ENGINE = MergeTree ORDER BY c1;
CREATE TABLE t3_05320 (c1 Int64, c2 DateTime64(9), c3 Nullable(Int64), c4 Int32) ENGINE = MergeTree ORDER BY c1;
CREATE TABLE t4_05320 (c1 Int64) ENGINE = MergeTree ORDER BY c1;

INSERT INTO t1_05320 SELECT number, 1, toDateTime64('2026-01-01 00:00:00', 9) + toIntervalMonth(number), toDateTime64('2026-01-01 00:00:00', 9) + toIntervalMonth(number + 1) FROM numbers(6);
INSERT INTO t2_05320 VALUES (1, 'b');
INSERT INTO t3_05320 SELECT number, toDateTime64('2026-01-01 00:00:00', 9) + toIntervalHour(number * 7), number % 10, number % 2 FROM numbers(500);
INSERT INTO t4_05320 SELECT number FROM numbers(5);

-- The JOIN has no equality key, so it can run only after the conversion.
SELECT 'issue';
SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2
    ON (x2.c2 >= toDateTime(x1.c3)) AND (x2.c2 <= toDateTime(x1.c4)) AND (x1.c2 = (SELECT c1 FROM t2_05320 WHERE c2 = 'a' LIMIT 1))
WHERE x2.c4 = 1 AND x2.c3 IN (SELECT c1 FROM t4_05320)
SETTINGS compatibility = '25.10';

SELECT 'left';
SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2 ON x2.c2 >= toDateTime(x1.c3)
WHERE x2.c4 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320);

SELECT 'left, IN (subquery) under OR';
SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2 ON x2.c2 >= toDateTime(x1.c3)
WHERE (x2.c4 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320)) OR x2.c4 = 3;

SELECT 'right';
SELECT count() FROM t3_05320 AS x2 RIGHT JOIN t1_05320 AS x1 ON x2.c2 >= toDateTime(x1.c3)
WHERE x2.c4 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320);

SELECT 'full';
SELECT count() FROM t1_05320 AS x1 FULL JOIN t3_05320 AS x2 ON x2.c2 >= toDateTime(x1.c3)
WHERE x2.c4 = 1 AND x1.c2 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320);

SELECT 'plan';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2 ON x1.c1 = x2.c1
    WHERE x2.c4 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320)
    SETTINGS enable_parallel_replicas = 0
) WHERE explain ILIKE '%Type: inner%';

-- The not-matched rows pass these filters, so the JOIN must stay LEFT: no row matches and all 6 rows are returned.
SELECT 'not converted';
SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2 ON x1.c1 + 1000 = x2.c1
WHERE x2.c1 NOT IN (SELECT c1 + 1 FROM t4_05320);
SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2 ON x1.c1 + 1000 = x2.c1
WHERE x2.c4 = 0 AND x2.c1 NOT IN (SELECT c1 + 1 FROM t4_05320);
SELECT count() FROM t1_05320 AS x1 LEFT JOIN t3_05320 AS x2 ON x1.c1 + 1000 = x2.c1
WHERE x2.c4 = 1 OR x2.c1 NOT IN (SELECT c1 + 1 FROM t4_05320);

SELECT 'any';
SELECT count(), sum(x2.c1) FROM t1_05320 AS x1 ANY LEFT JOIN t3_05320 AS x2 ON x1.c1 = x2.c1
WHERE x2.c4 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320);
SELECT count(), sum(x2.c1) FROM t1_05320 AS x1 ANY LEFT JOIN t3_05320 AS x2 ON x1.c1 = x2.c1
WHERE x2.c4 = 1 AND x2.c1 IN (SELECT c1 FROM t4_05320)
SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 0, query_plan_convert_outer_join_to_inner_join = 0;

DROP TABLE t1_05320;
DROP TABLE t2_05320;
DROP TABLE t3_05320;
DROP TABLE t4_05320;
