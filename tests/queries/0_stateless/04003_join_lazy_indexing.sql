-- Test lazy columns indexing in joins with small limit

SET query_plan_min_columns_for_join_lazy_indexing = 1;
SET query_plan_optimize_join_order_limit = 1;
SET query_plan_join_swap_table = false;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t3 (a UInt64, c String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 SELECT number, number, toString(number) FROM numbers(10000);
INSERT INTO t2 SELECT number, 50, toString(number) FROM numbers(100);
INSERT INTO t3 SELECT number, toString(number) FROM numbers(100);

-- Join followed by LIMIT
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 5
);

-- Join followed by filter and LIMIT
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b < t2.b LIMIT 5
);

-- Join followed by ORDER BY + LIMIT
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a LIMIT 5
);

-- Two joins
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a == t3.a
);

-- Filter between joins
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a == t3.a WHERE t1.b < t2.b
);

-- Two joins followed by LIMIT
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a == t3.a LIMIT 5
);

-- LIMIT between joins
SELECT count() FROM (
    SELECT *
    FROM (
        SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 5
    ) sub
    JOIN t3 ON sub.a = t3.a
);

-- Join with constant columns
SELECT *
FROM (SELECT 1 AS a, 2 AS b, 3 AS c) AS t1
JOIN (SELECT 4 AS c, 5 AS b, 1 AS a) AS t2
ON t2.a = t1.a AND ( t1.b > 0 OR t2.b > 0 )
LIMIT 1
SETTINGS enable_analyzer = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
