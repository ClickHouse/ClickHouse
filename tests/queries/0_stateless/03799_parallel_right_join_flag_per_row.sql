DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 UInt64, c1 UInt64) ENGINE = MergeTree() ORDER BY (c0);
INSERT INTO t0 VALUES (2, 20), (1, 30);
CREATE TABLE t1 (c0 UInt64, c1 UInt64) ENGINE = MergeTree() ORDER BY (c0);
INSERT INTO t1 VALUES (1, 10);

SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;

SELECT
    *
FROM t1
RIGHT JOIN t0
    ON (t0.c0 = t1.c0)
    AND (t0.c1 >= t1.c1)
ORDER BY t0.c0
SETTINGS join_algorithm='hash';

SELECT '-';

SELECT
    *
FROM t1
RIGHT JOIN t0
    ON (t0.c0 = t1.c0)
    AND (t0.c1 >= t1.c1)
ORDER BY t0.c0
SETTINGS join_algorithm='parallel_hash';


SELECT '-';

SELECT
    *
FROM t1
RIGHT JOIN t0
    ON (t0.c0 = t1.c0)
    AND (t0.c1 >= t1.c1)
WHERE t0.c0 = 2
SETTINGS join_algorithm='parallel_hash', query_plan_filter_push_down = 1;

SELECT '-';


SELECT
    *
FROM t1
RIGHT JOIN t0
    ON (t0.c0 = t1.c0)
    AND (t0.c1 >= t1.c1)
WHERE t0.c0 = 2
SETTINGS join_algorithm='parallel_hash', query_plan_filter_push_down = 0;
