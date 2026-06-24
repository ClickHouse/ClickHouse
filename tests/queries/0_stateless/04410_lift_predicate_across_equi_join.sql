-- Tags: long
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS lift_orders;
DROP TABLE IF EXISTS lift_lineitem;
DROP TABLE IF EXISTS lift_mem;

CREATE TABLE lift_orders   (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE lift_lineitem (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE lift_mem      (orderkey UInt64) ENGINE = Memory;

INSERT INTO lift_orders   SELECT number, number % 1000, toString(number) FROM numbers(1000000);
INSERT INTO lift_lineitem SELECT number, number % 1000, toString(number) FROM numbers(1000000);
INSERT INTO lift_mem      SELECT number FROM numbers(1000);

-- INNER JOIN, equality predicate on left subquery, target side gets substituted predicate
SELECT 'inner eq',
       countIf(explain LIKE '%equals(__table3.orderkey, 12345_UInt16)%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 12345) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- LEFT JOIN, range predicate
SELECT 'left between',
       countIf(explain LIKE '%greaterOrEquals(__table3.orderkey, 100000_UInt32)%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT sum(l.orderkey)
    FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 100000 AND 100100) AS o
    LEFT JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- Predicate on non-key column, nothing to lift
SELECT 'non-key',
       countIf(explain LIKE '%equals(__table3.orderkey%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE payload = 'x') AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- FULL JOIN: lift unsound, skip
SELECT 'full join',
       countIf(explain LIKE '%equals(__table3.orderkey%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 1) AS o
    FULL JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- Setting off
SELECT 'setting off',
       countIf(explain LIKE '%equals(__table3.orderkey%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 1) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS query_plan_lift_predicate_across_join = 0
);

-- Multi-clause JOIN, filter on orderkey (one of two equi-keys) should still lift via that key
SELECT 'multi-clause',
       countIf(explain LIKE '%equals(__table3.orderkey, 42_UInt8)%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey AND o.custkey = l.custkey
);

-- Target side is not indexed (Memory), bail
SELECT 'non-indexed target',
       countIf(explain LIKE '%Lifted equi-join filter%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 1) AS o
    INNER JOIN lift_mem AS l ON o.orderkey = l.orderkey
);

-- LEFT JOIN with filter on RIGHT subquery, lifting RIGHT->LEFT would drop unmatched left rows, skip
SELECT 'left, filter on rhs',
       countIf(explain LIKE '%Lifted equi-join filter%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM lift_orders AS o
    LEFT JOIN (SELECT * FROM lift_lineitem WHERE orderkey = 1) AS l ON o.orderkey = l.orderkey
);

-- Non-deterministic predicate: lift would produce a different filter on the target side
SELECT 'non-deterministic',
       countIf(explain LIKE '%Lifted equi-join filter%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = randConstant() % 100) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- Correctness: result equals reference plan (no lift). Same answer with and without setting
SELECT 'result match',
       (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_lift_predicate_across_join = 0);

-- Correctness on LEFT JOIN with non-matching keys
SELECT 'left correctness',
       (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 999990 AND 999999) AS o
        LEFT JOIN lift_lineitem AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 999990 AND 999999) AS o
        LEFT JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_lift_predicate_across_join = 0);

DROP TABLE lift_orders;
DROP TABLE lift_lineitem;
DROP TABLE lift_mem;
