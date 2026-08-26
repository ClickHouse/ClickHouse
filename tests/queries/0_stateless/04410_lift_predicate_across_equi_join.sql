-- Tags: long
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
-- Under parallel replicas the reads are remote, the pass bails and the EXPLAIN output changes
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS lift_orders;
DROP TABLE IF EXISTS lift_lineitem;
DROP TABLE IF EXISTS lift_mem;
DROP TABLE IF EXISTS lift_two_key;

CREATE TABLE lift_orders   (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE lift_lineitem (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE lift_mem      (orderkey UInt64) ENGINE = Memory;
CREATE TABLE lift_two_key  (orderkey UInt64, custkey UInt64) ENGINE = MergeTree ORDER BY (orderkey, custkey);

INSERT INTO lift_orders   SELECT number, number % 1000, toString(number) FROM numbers(1000000);
INSERT INTO lift_lineitem SELECT number, number % 1000, toString(number) FROM numbers(1000000);
-- Keys present only on the left side, for the unmatched-rows case below
INSERT INTO lift_orders   SELECT number, number % 1000, toString(number) FROM numbers(1000000, 10);
INSERT INTO lift_mem      SELECT number FROM numbers(1000);
INSERT INTO lift_two_key  SELECT number, number % 100 FROM numbers(10000);

-- 1 occurrence = source side only, 2 = lifted to the target too

-- INNER JOIN, equality on the left subquery
SELECT 'inner eq',
       countIf(explain LIKE '%ilter column:%orderkey = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 12345) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- LEFT JOIN, range predicate
SELECT 'left between',
       countIf(explain LIKE '%orderkey >= 100000%' OR explain LIKE '%orderkey <= 100100%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT sum(l.orderkey)
    FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 100000 AND 100100) AS o
    LEFT JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- Predicate on non-key column, nothing to lift
SELECT 'non-key',
       countIf(explain LIKE '%ilter column:%orderkey =%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE payload = 'x') AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- FULL JOIN, lift unsound, skip (source-side filter only)
SELECT 'full join',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 1) AS o
    FULL JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- Setting off (source-side filter only)
SELECT 'setting off',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 1) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS query_plan_lift_predicate_across_join = 0
);

-- Multi-clause JOIN, filter on orderkey (one of two equi-keys) still lifts via that key
SELECT 'multi-clause',
       countIf(explain LIKE '%ilter column:%orderkey = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey AND o.custkey = l.custkey
);

-- Target side is not indexed (Memory), bail (source-side filter only)
SELECT 'non-indexed target',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 1) AS o
    INNER JOIN lift_mem AS l ON o.orderkey = l.orderkey
);

-- LEFT JOIN with filter on RIGHT subquery, lifting RIGHT->LEFT would drop unmatched left rows, skip
SELECT 'left, filter on rhs',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM lift_orders AS o
    LEFT JOIN (SELECT * FROM lift_lineitem WHERE orderkey = 1) AS l ON o.orderkey = l.orderkey
);

-- Non-deterministic predicate: lifting it would filter the target by a different value.
-- Not `randConstant`, which is folded to a literal before the pass and would be fine to lift
SELECT 'non-deterministic',
       countIf(explain LIKE '%ilter column:%orderkey =%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = rand() % 100) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
);

-- Same answer with and without the lift
SELECT 'result match',
       (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_lift_predicate_across_join = 0);

-- LEFT JOIN where 10 of the 15 left rows have no match. Compare the joined values, not just the
-- count: dropping matching target rows would leave the count intact and only lower the sum
WITH
    (SELECT (count(), countIf(l.orderkey = 0), sum(l.orderkey))
     FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 999995 AND 1000009) AS o
     LEFT JOIN lift_lineitem AS l ON o.orderkey = l.orderkey) AS with_lift,
    (SELECT (count(), countIf(l.orderkey = 0), sum(l.orderkey))
     FROM (SELECT * FROM lift_orders WHERE orderkey BETWEEN 999995 AND 1000009) AS o
     LEFT JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
     SETTINGS query_plan_lift_predicate_across_join = 0) AS without_lift
SELECT 'left unmatched keys', with_lift, with_lift = without_lift;

-- Computed equi-key: lifting it would reference a column missing from the target child's header
SELECT 'computed key',
       countIf(explain LIKE '%ilter column:%orderkey = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
    INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey + 1
);

SELECT 'computed key correctness',
       (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
        INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey + 1)
     - (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
        INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey + 1
        SETTINGS query_plan_lift_predicate_across_join = 0);

-- Target subquery computes the join key under the primary key's name: `orderkey` there is not
-- the table's `orderkey`, so the copy could not prune anything
SELECT 'computed target key',
       countIf(explain LIKE '%ilter column:%orderkey = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
    INNER JOIN (SELECT orderkey + 1000000 AS orderkey FROM lift_lineitem) AS l ON o.orderkey = l.orderkey
);

SELECT 'computed target key correctness',
       (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
        INNER JOIN (SELECT orderkey + 1000000 AS orderkey FROM lift_lineitem) AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM lift_orders WHERE orderkey = 42) AS o
        INNER JOIN (SELECT orderkey + 1000000 AS orderkey FROM lift_lineitem) AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_lift_predicate_across_join = 0);

-- The target side already carries a pushed-down filter above its rename step: the key must still
-- resolve through it to the primary key. Count granules instead of filters, so that the lift is
-- only credited when the target read actually prunes
WITH
    (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')))
     FROM (
         EXPLAIN PLAN indexes=1
         SELECT count()
         FROM (SELECT * FROM lift_orders WHERE orderkey = 4242) AS o
         INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
         WHERE l.custkey != 999999
     )) AS with_lift,
    (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')))
     FROM (
         EXPLAIN PLAN indexes=1
         SELECT count()
         FROM (SELECT * FROM lift_orders WHERE orderkey = 4242) AS o
         INNER JOIN lift_lineitem AS l ON o.orderkey = l.orderkey
         WHERE l.custkey != 999999
         SETTINGS query_plan_lift_predicate_across_join = 0
     )) AS without_lift
SELECT 'filtered target prunes', with_lift < without_lift;

-- Same shape, but the target subquery computes the key under the primary key's name: no lift
SELECT 'filtered computed target key',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_orders WHERE orderkey = 4242) AS o
    INNER JOIN (SELECT orderkey + 1000000 AS orderkey, payload FROM lift_lineitem WHERE payload != '') AS l
        ON o.orderkey = l.orderkey
);

-- Both keys are in the target primary key, but `KeyCondition` cannot use `key = key`, so a
-- key-vs-key predicate must stay on the source side
SELECT 'key vs key',
       countIf(explain LIKE '%ilter column:%orderkey = %custkey%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_two_key WHERE orderkey = custkey) AS a
    INNER JOIN lift_two_key AS b ON a.orderkey = b.orderkey AND a.custkey = b.custkey
);

SELECT 'key vs key correctness',
       (SELECT count() FROM (SELECT * FROM lift_two_key WHERE orderkey = custkey) AS a
        INNER JOIN lift_two_key AS b ON a.orderkey = b.orderkey AND a.custkey = b.custkey)
     - (SELECT count() FROM (SELECT * FROM lift_two_key WHERE orderkey = custkey) AS a
        INNER JOIN lift_two_key AS b ON a.orderkey = b.orderkey AND a.custkey = b.custkey
        SETTINGS query_plan_lift_predicate_across_join = 0);

DROP TABLE lift_orders;
DROP TABLE lift_lineitem;
DROP TABLE lift_mem;
DROP TABLE lift_two_key;
