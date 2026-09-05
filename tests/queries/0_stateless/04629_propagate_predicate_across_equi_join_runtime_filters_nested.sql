SET enable_analyzer = 1;
-- Under parallel replicas the reads are remote and the pass bails
SET enable_parallel_replicas = 0;
-- Pinned because the test asserts the runtime filter is built; the default threshold skips it
-- when the probe side is small
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS prop_rf_orders;
DROP TABLE IF EXISTS prop_rf_lineitem;
DROP TABLE IF EXISTS prop_rf_customer;

CREATE TABLE prop_rf_orders   (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY custkey;
CREATE TABLE prop_rf_lineitem (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE prop_rf_customer (custkey UInt64, name String) ENGINE = MergeTree ORDER BY custkey;

INSERT INTO prop_rf_orders   SELECT number, number % 1000, toString(number) FROM numbers(10000);
INSERT INTO prop_rf_lineitem SELECT number, number % 1000, toString(number) FROM numbers(10000);
INSERT INTO prop_rf_customer SELECT number, toString(number) FROM numbers(1000);

-- The pass must run before the runtime-filter wrappers hide the source filter
SELECT 'runtime filters on',
       countIf(explain LIKE '%ilter column:%orderkey = 1234%') >= 2
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_rf_orders WHERE orderkey = 1234) AS o
    INNER JOIN prop_rf_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS enable_join_runtime_filters = 1
);

-- Correctness with runtime filters on
SELECT 'runtime filters on correctness',
       (SELECT count() FROM (SELECT * FROM prop_rf_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN prop_rf_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS enable_join_runtime_filters = 1)
     - (SELECT count() FROM (SELECT * FROM prop_rf_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN prop_rf_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS enable_join_runtime_filters = 1, query_plan_propagate_predicate_across_join = 0);

-- Nested join: pushdown already sunk `c.custkey = 42` below it, so nothing is copied
SELECT 'nested join correctness',
       (SELECT count()
        FROM prop_rf_orders AS o
        INNER JOIN (
            SELECT l.orderkey AS orderkey, c.custkey AS ck
            FROM prop_rf_lineitem AS l
            INNER JOIN prop_rf_customer AS c ON l.custkey = c.custkey
            WHERE c.custkey = 42
        ) AS rhs ON o.custkey = rhs.ck)
     - (SELECT count()
        FROM prop_rf_orders AS o
        INNER JOIN (
            SELECT l.orderkey AS orderkey, c.custkey AS ck
            FROM prop_rf_lineitem AS l
            INNER JOIN prop_rf_customer AS c ON l.custkey = c.custkey
            WHERE c.custkey = 42
        ) AS rhs ON o.custkey = rhs.ck
        SETTINGS query_plan_propagate_predicate_across_join = 0);

DROP TABLE prop_rf_orders;
DROP TABLE prop_rf_lineitem;
DROP TABLE prop_rf_customer;
