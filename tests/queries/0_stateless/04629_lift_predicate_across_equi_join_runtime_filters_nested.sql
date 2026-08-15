-- Tags: long
SET enable_analyzer = 1;
-- The lift targets local MergeTree reads; under parallel replicas the plan reads through
-- remote-replica steps and the pass correctly bails, changing the EXPLAIN output
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS lift_rf_orders;
DROP TABLE IF EXISTS lift_rf_lineitem;
DROP TABLE IF EXISTS lift_rf_customer;

CREATE TABLE lift_rf_orders   (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY custkey;
CREATE TABLE lift_rf_lineitem (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE lift_rf_customer (custkey UInt64, name String) ENGINE = MergeTree ORDER BY custkey;

INSERT INTO lift_rf_orders   SELECT number, number % 1000, toString(number) FROM numbers(1000000);
INSERT INTO lift_rf_lineitem SELECT number, number % 1000, toString(number) FROM numbers(1000000);
INSERT INTO lift_rf_customer SELECT number, toString(number) FROM numbers(1000);

-- Counting occurrences of the predicate in filter steps: 1 = source side only, >= 2 = lifted to target too

-- Default path: join runtime filters left enabled. The lift must run before the runtime-filter
-- wrappers are added, so the source predicate is still lifted to the target side
SELECT 'runtime filters on',
       countIf(explain LIKE '%ilter column:%orderkey = 12345%') >= 2
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_rf_orders WHERE orderkey = 12345) AS o
    INNER JOIN lift_rf_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS enable_join_runtime_filters = 1
);

-- Correctness on the default runtime-filter path
SELECT 'runtime filters on correctness',
       (SELECT count() FROM (SELECT * FROM lift_rf_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN lift_rf_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS enable_join_runtime_filters = 1)
     - (SELECT count() FROM (SELECT * FROM lift_rf_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN lift_rf_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS enable_join_runtime_filters = 1, query_plan_lift_predicate_across_join = 0);

-- Nested join: the source predicate is written on an inner-child key (`c.custkey`), which is only
-- transitively equivalent to the outer join key (`o.custkey = l.custkey`, `l.custkey = c.custkey`)
SELECT 'nested join transitive key',
       countIf(explain LIKE '%ilter column:%custkey = 42%') >= 2
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM lift_rf_orders AS o
    INNER JOIN (
        SELECT l.orderkey AS orderkey, c.custkey AS ck
        FROM lift_rf_lineitem AS l
        INNER JOIN lift_rf_customer AS c ON l.custkey = c.custkey
        WHERE c.custkey = 42
    ) AS rhs ON o.custkey = rhs.ck
);

SELECT 'nested join correctness',
       (SELECT count()
        FROM lift_rf_orders AS o
        INNER JOIN (
            SELECT l.orderkey AS orderkey, c.custkey AS ck
            FROM lift_rf_lineitem AS l
            INNER JOIN lift_rf_customer AS c ON l.custkey = c.custkey
            WHERE c.custkey = 42
        ) AS rhs ON o.custkey = rhs.ck)
     - (SELECT count()
        FROM lift_rf_orders AS o
        INNER JOIN (
            SELECT l.orderkey AS orderkey, c.custkey AS ck
            FROM lift_rf_lineitem AS l
            INNER JOIN lift_rf_customer AS c ON l.custkey = c.custkey
            WHERE c.custkey = 42
        ) AS rhs ON o.custkey = rhs.ck
        SETTINGS query_plan_lift_predicate_across_join = 0);

DROP TABLE lift_rf_orders;
DROP TABLE lift_rf_lineitem;
DROP TABLE lift_rf_customer;
