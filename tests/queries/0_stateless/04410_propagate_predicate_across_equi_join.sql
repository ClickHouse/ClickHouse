-- Tags: long
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
-- Under parallel replicas the reads are remote and the pass bails
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS prop_orders;
DROP TABLE IF EXISTS prop_lineitem;
DROP TABLE IF EXISTS prop_mem;
DROP TABLE IF EXISTS prop_two_key;

CREATE TABLE prop_orders   (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE prop_lineitem (orderkey UInt64, custkey UInt64, payload String) ENGINE = MergeTree ORDER BY orderkey;
CREATE TABLE prop_mem      (orderkey UInt64) ENGINE = Memory;
CREATE TABLE prop_two_key  (orderkey UInt64, custkey UInt64) ENGINE = MergeTree ORDER BY (orderkey, custkey);

INSERT INTO prop_orders   SELECT number, number % 1000, toString(number) FROM numbers(1000000);
INSERT INTO prop_lineitem SELECT number, number % 1000, toString(number) FROM numbers(1000000);
-- Keys only on the left, for the unmatched-rows case below
INSERT INTO prop_orders   SELECT number, number % 1000, toString(number) FROM numbers(1000000, 10);
INSERT INTO prop_mem      SELECT number FROM numbers(1000);
INSERT INTO prop_two_key  SELECT number, number % 100 FROM numbers(10000);

-- 1 occurrence = source side only, 2 = copied to the target too

-- INNER JOIN, equality on the left subquery
SELECT 'inner eq',
       countIf(explain LIKE '%ilter column:%orderkey = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 12345) AS o
    INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

-- LEFT JOIN, range predicate
SELECT 'left between',
       countIf(explain LIKE '%orderkey >= 100000%' OR explain LIKE '%orderkey <= 100100%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT sum(l.orderkey)
    FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 100000 AND 100100) AS o
    LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

-- Predicate on a non-key column, nothing to copy
SELECT 'non-key',
       countIf(explain LIKE '%ilter column:%orderkey =%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE payload = 'x') AS o
    INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

-- FULL JOIN is unsound to copy across
SELECT 'full join',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 1) AS o
    FULL JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

-- Setting off (source-side filter only)
SELECT 'setting off',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 1) AS o
    INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS query_plan_propagate_predicate_across_join = 0
);

-- Multi-clause JOIN, the filter still travels via its own key
SELECT 'multi-clause',
       countIf(explain LIKE '%ilter column:%orderkey = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
    INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey AND o.custkey = l.custkey
);

-- Target is not indexed (Memory), bail
SELECT 'non-indexed target',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 1) AS o
    INNER JOIN prop_mem AS l ON o.orderkey = l.orderkey
);

-- LEFT JOIN, R->L would drop unmatched left rows
SELECT 'left, filter on rhs',
       countIf(explain LIKE '%ilter column:%orderkey = 1%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM prop_orders AS o
    LEFT JOIN (SELECT * FROM prop_lineitem WHERE orderkey = 1) AS l ON o.orderkey = l.orderkey
);

-- Non-deterministic predicate: the target would be filtered by a different value
SELECT 'non-deterministic',
       countIf(explain LIKE '%ilter column:%orderkey =%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = rand() % 100) AS o
    INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

-- Same answer either way
SELECT 'result match',
       (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 100 AND 200) AS o
        INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_propagate_predicate_across_join = 0);

-- LEFT JOIN where 10 of the 15 left rows have no match. Compare the joined values, not the count:
-- dropping matching target rows would leave the count intact and only lower the sum
WITH
    (SELECT (count(), countIf(l.orderkey = 0), sum(l.orderkey))
     FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 999995 AND 1000009) AS o
     LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey) AS with_pass,
    (SELECT (count(), countIf(l.orderkey = 0), sum(l.orderkey))
     FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 999995 AND 1000009) AS o
     LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
     SETTINGS query_plan_propagate_predicate_across_join = 0) AS without_pass
SELECT 'left unmatched keys', with_pass, with_pass = without_pass;

-- Computed equi-key: the copy would reference a column missing from the target header
SELECT 'computed key',
       countIf(explain LIKE '%ilter column:%orderkey = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
    INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey + 1
);

SELECT 'computed key correctness',
       (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
        INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey + 1)
     - (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
        INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey + 1
        SETTINGS query_plan_propagate_predicate_across_join = 0);

-- The target subquery computes `orderkey`, so the copy could not prune anything
SELECT 'computed target key',
       countIf(explain LIKE '%ilter column:%orderkey = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
    INNER JOIN (SELECT orderkey + 1000000 AS orderkey FROM prop_lineitem) AS l ON o.orderkey = l.orderkey
);

SELECT 'computed target key correctness',
       (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
        INNER JOIN (SELECT orderkey + 1000000 AS orderkey FROM prop_lineitem) AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 42) AS o
        INNER JOIN (SELECT orderkey + 1000000 AS orderkey FROM prop_lineitem) AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_propagate_predicate_across_join = 0);

-- Target with its own pushed-down filter. Count granules, so the copy is credited only when it prunes
WITH
    (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')))
     FROM (
         EXPLAIN PLAN indexes=1
         SELECT count()
         FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
         INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
         WHERE l.custkey != 999999
     )) AS with_pass,
    (SELECT sum(toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')))
     FROM (
         EXPLAIN PLAN indexes=1
         SELECT count()
         FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
         INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
         WHERE l.custkey != 999999
         SETTINGS query_plan_propagate_predicate_across_join = 0
     )) AS without_pass
SELECT 'filtered target prunes', with_pass < without_pass;

-- Same shape, but the target subquery computes the key under the primary key's name
SELECT 'filtered computed target key',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    INNER JOIN (SELECT orderkey + 1000000 AS orderkey, payload FROM prop_lineitem WHERE payload != '') AS l
        ON o.orderkey = l.orderkey
);

-- Filter on the right input: the copy runs in the other direction
SELECT 'inner eq rhs',
       countIf(explain LIKE '%ilter column:%orderkey = 777%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM prop_orders AS o
    INNER JOIN (SELECT * FROM prop_lineitem WHERE orderkey = 777) AS l ON o.orderkey = l.orderkey
);

-- Both inputs filtered: each direction sees the other side as it was before the first copy
SELECT 'both sides propagate',
       countIf(explain LIKE '%ilter column:%orderkey = 555%'),
       countIf(explain LIKE '%ilter column:%custkey = 7%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_two_key WHERE orderkey = 555) AS a
    INNER JOIN (SELECT * FROM prop_two_key WHERE custkey = 7) AS b
        ON a.orderkey = b.orderkey AND a.custkey = b.custkey
);

SELECT 'both sides propagate correctness',
       (SELECT count() FROM (SELECT * FROM prop_two_key WHERE orderkey = 555) AS a
        INNER JOIN (SELECT * FROM prop_two_key WHERE custkey = 7) AS b
            ON a.orderkey = b.orderkey AND a.custkey = b.custkey)
     - (SELECT count() FROM (SELECT * FROM prop_two_key WHERE orderkey = 555) AS a
        INNER JOIN (SELECT * FROM prop_two_key WHERE custkey = 7) AS b
            ON a.orderkey = b.orderkey AND a.custkey = b.custkey
        SETTINGS query_plan_propagate_predicate_across_join = 0);

-- `ANY` keeps at most one match per key, and a key predicate takes whole key groups
SELECT 'any inner',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    ANY INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

SELECT 'any inner correctness',
       (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
        ANY INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
        ANY INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_propagate_predicate_across_join = 0);

-- `ANY LEFT` keeps every left row, so the copy must not remove a match or an unmatched row
SELECT 'any left',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    ANY LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

WITH
    (SELECT (count(), countIf(l.orderkey = 0), sum(l.orderkey))
     FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 999995 AND 1000009) AS o
     ANY LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey) AS with_pass,
    (SELECT (count(), countIf(l.orderkey = 0), sum(l.orderkey))
     FROM (SELECT * FROM prop_orders WHERE orderkey BETWEEN 999995 AND 1000009) AS o
     ANY LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
     SETTINGS query_plan_propagate_predicate_across_join = 0) AS without_pass
SELECT 'any left unmatched keys', with_pass, with_pass = without_pass;

-- `SEMI` only asks whether a match exists, and a key predicate keeps whole key groups
SELECT 'semi left',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    SEMI LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
);

SELECT 'semi left correctness',
       (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
        SEMI LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey)
     - (SELECT count() FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
        SEMI LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
        SETTINGS query_plan_propagate_predicate_across_join = 0);

-- `any_join_distinct_right_table_keys` turns these into `SEMI LEFT` and `RightAny`
SELECT 'legacy any inner',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    ANY INNER JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS any_join_distinct_right_table_keys = 1
);

SELECT 'legacy any left',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    ANY LEFT JOIN prop_lineitem AS l ON o.orderkey = l.orderkey
    SETTINGS any_join_distinct_right_table_keys = 1
);

-- Index analysis cannot reach past `DISTINCT`, so the predicate stays on the source side
SELECT 'distinct target',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    INNER JOIN (SELECT DISTINCT orderkey FROM prop_lineitem) AS l ON o.orderkey = l.orderkey
);

-- A subquery `ORDER BY` is removed before the pass runs, so this one does propagate
SELECT 'sorted target',
       countIf(explain LIKE '%ilter column:%orderkey = 4242%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_orders WHERE orderkey = 4242) AS o
    INNER JOIN (SELECT * FROM prop_lineitem ORDER BY custkey) AS l ON o.orderkey = l.orderkey
);

-- `KeyCondition` cannot use `key = key`, so it must stay on the source side
SELECT 'key vs key',
       countIf(explain LIKE '%ilter column:%orderkey = %custkey%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_two_key WHERE orderkey = custkey) AS a
    INNER JOIN prop_two_key AS b ON a.orderkey = b.orderkey AND a.custkey = b.custkey
);

SELECT 'key vs key correctness',
       (SELECT count() FROM (SELECT * FROM prop_two_key WHERE orderkey = custkey) AS a
        INNER JOIN prop_two_key AS b ON a.orderkey = b.orderkey AND a.custkey = b.custkey)
     - (SELECT count() FROM (SELECT * FROM prop_two_key WHERE orderkey = custkey) AS a
        INNER JOIN prop_two_key AS b ON a.orderkey = b.orderkey AND a.custkey = b.custkey
        SETTINGS query_plan_propagate_predicate_across_join = 0);

DROP TABLE prop_orders;
DROP TABLE prop_lineitem;
DROP TABLE prop_mem;
DROP TABLE prop_two_key;
