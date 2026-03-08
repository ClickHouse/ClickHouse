-- Test lazy columns indexing in joins with small limit

SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t3 (a UInt64, b String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 SELECT number, toString(number) FROM numbers(10000);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t3 SELECT number, toString(number) FROM numbers(100);

-- Join followed by LIMIT
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 5
) SETTINGS query_plan_optimize_join_lazy_indexing = 0;

SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 5
) SETTINGS query_plan_optimize_join_lazy_indexing = 1;

-- Two joins followed by LIMIT
SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a == t3.a LIMIT 5
) SETTINGS query_plan_optimize_join_lazy_indexing = 0;

SELECT count(*) FROM (
    SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a == t3.a LIMIT 5
) SETTINGS query_plan_optimize_join_lazy_indexing = 1;

-- Limit between joins
SELECT count() FROM (
    SELECT *
    FROM (
        SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 5
    ) sub
    JOIN t3 ON sub.a = t3.a
) SETTINGS query_plan_optimize_join_lazy_indexing = 0;

SELECT count() FROM (
    SELECT *
    FROM (
        SELECT * FROM t1 JOIN t2 ON t1.a = t2.a LIMIT 5
    ) sub
    JOIN t3 ON sub.a = t3.a
) SETTINGS query_plan_optimize_join_lazy_indexing = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
