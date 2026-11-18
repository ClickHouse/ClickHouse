-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100781
-- The `optimize_read_in_order` optimization incorrectly propagated the sorted
-- property through grace hash join, which scatters rows into buckets by hash
-- and destroys the input order.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key UInt32, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (key UInt32, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 VALUES (1, 10, 'alpha'), (2, 15, 'beta'), (3, 20, 'gamma');
INSERT INTO t2 VALUES (1, 5, 'ALPHA'), (2, 10, 'beta'), (4, 25, 'delta');

SET enable_analyzer = 1;
SET allow_experimental_join_condition = 1;
SET join_use_nulls = 0;
SET join_algorithm = 'grace_hash';
SET grace_hash_join_initial_buckets = 8;

-- Single LEFT JOIN with non-equality condition
SELECT t1.key FROM t1
LEFT JOIN t2 ON t1.key = t2.key AND t1.attr != t2.attr
ORDER BY t1.key;

-- Two LEFT JOINs with non-equality conditions (original reproducer)
SELECT t1.key FROM t1
LEFT JOIN t2 ON t1.key = t2.key AND t1.attr != t2.attr
LEFT JOIN (SELECT * FROM VALUES('key UInt64, a UInt64', (0,10),(1,100),(2,1000))) t3
    ON t1.key = t3.key AND t2.key = t3.key AND t3.a != t1.a AND t3.a != t2.a
ORDER BY t1.key;

DROP TABLE t1;
DROP TABLE t2;
