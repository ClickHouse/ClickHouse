-- `k` is not in the target's primary key, so nothing can be pruned: the copy only shrinks the input
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS prop_nopk_big;
DROP TABLE IF EXISTS prop_nopk_small;

CREATE TABLE prop_nopk_big (k UInt64, pad UInt64) ENGINE = MergeTree ORDER BY pad;
CREATE TABLE prop_nopk_small (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO prop_nopk_big SELECT number, number FROM numbers(10000);
INSERT INTO prop_nopk_small SELECT number FROM numbers(1000);

-- A comparison costs a fraction of a probe, so it is copied even without an index
SELECT 'comparison',
       countIf(explain LIKE '%ilter column:%k = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_small WHERE k = 42) AS s
    INNER JOIN prop_nopk_big AS b ON s.k = b.k
);

-- A set lookup is an order of magnitude more expensive, so off-index it stays on the source side
SELECT 'in set',
       countIf(explain LIKE '%ilter column:%k IN (42, 43)%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_small WHERE k IN (42, 43)) AS s
    INNER JOIN prop_nopk_big AS b ON s.k = b.k
);

-- On a primary key column a set lookup is copied, because pruning pays for it
SELECT 'in set on a key column',
       countIf(explain LIKE '%ilter column:%k IN (42, 43)%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_big WHERE k IN (42, 43)) AS b
    INNER JOIN prop_nopk_small AS s ON b.k = s.k
);

-- ... but not when index analysis is off, because then it prunes nothing
SELECT 'in set on a key column, no index analysis',
       countIf(explain LIKE '%ilter column:%k IN (42, 43)%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_big WHERE k IN (42, 43)) AS b
    INNER JOIN prop_nopk_small AS s ON b.k = s.k
    SETTINGS use_primary_key = 0
);

SELECT 'correctness',
       (SELECT count() FROM (SELECT * FROM prop_nopk_small WHERE k = 42) AS s
        INNER JOIN prop_nopk_big AS b ON s.k = b.k)
     - (SELECT count() FROM (SELECT * FROM prop_nopk_small WHERE k = 42) AS s
        INNER JOIN prop_nopk_big AS b ON s.k = b.k
        SETTINGS query_plan_propagate_predicate_across_join = 0);

DROP TABLE prop_nopk_big;
DROP TABLE prop_nopk_small;

-- A fixed-size key of at most 32 bytes is cheap to compare, a wider or variable-size one is not
DROP TABLE IF EXISTS prop_nopk_types_big;
DROP TABLE IF EXISTS prop_nopk_types_small;

CREATE TABLE prop_nopk_types_big (u UUID, ip IPv6, fs FixedString(32), wfs FixedString(33), n Nullable(UInt64), s String, pad UInt64) ENGINE = MergeTree ORDER BY pad;
CREATE TABLE prop_nopk_types_small (u UUID, ip IPv6, fs FixedString(32), wfs FixedString(33), n Nullable(UInt64), s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO prop_nopk_types_big SELECT toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number % 1000), 12, '0'))), toIPv6(IPv4NumToString(toUInt32(number % 1000))), leftPad(toString(number % 1000), 3, '0'), leftPad(toString(number % 1000), 3, '0'), number % 1000, toString(number % 1000), number FROM numbers(10000);
INSERT INTO prop_nopk_types_small SELECT toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0'))), toIPv6(IPv4NumToString(toUInt32(number))), leftPad(toString(number), 3, '0'), leftPad(toString(number), 3, '0'), number, toString(number) FROM numbers(1000);

SELECT 'UUID',
       countIf(explain LIKE '%ilter column:%u = %000000000042%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_types_small WHERE u = toUUID('00000000-0000-0000-0000-000000000042')) AS s
    INNER JOIN prop_nopk_types_big AS b ON s.u = b.u
);

SELECT 'IPv6',
       countIf(explain LIKE '%ilter column:%ip = %0.0.0.42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_types_small WHERE ip = toIPv6('::ffff:0.0.0.42')) AS s
    INNER JOIN prop_nopk_types_big AS b ON s.ip = b.ip
);

SELECT 'FixedString',
       countIf(explain LIKE '%ilter column:%fs = %042%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_types_small WHERE fs = '042') AS s
    INNER JOIN prop_nopk_types_big AS b ON s.fs = b.fs
);

SELECT 'wide FixedString',
       countIf(explain LIKE '%ilter column:%wfs = %042%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_types_small WHERE wfs = '042') AS s
    INNER JOIN prop_nopk_types_big AS b ON s.wfs = b.wfs
);

SELECT 'Nullable',
       countIf(explain LIKE '%ilter column:%n = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_types_small WHERE n = 42) AS s
    INNER JOIN prop_nopk_types_big AS b ON s.n = b.n
);

SELECT 'String',
       countIf(explain LIKE '%ilter column:%s = %42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_types_small WHERE s = '42') AS s
    INNER JOIN prop_nopk_types_big AS b ON s.s = b.s
);

SELECT 'types correctness',
       (SELECT count() FROM (SELECT * FROM prop_nopk_types_small WHERE u = toUUID('00000000-0000-0000-0000-000000000042')) AS s
        INNER JOIN prop_nopk_types_big AS b ON s.u = b.u),
       (SELECT count() FROM (SELECT * FROM prop_nopk_types_small WHERE ip = toIPv6('::ffff:0.0.0.42')) AS s
        INNER JOIN prop_nopk_types_big AS b ON s.ip = b.ip),
       (SELECT count() FROM (SELECT * FROM prop_nopk_types_small WHERE fs = '042') AS s
        INNER JOIN prop_nopk_types_big AS b ON s.fs = b.fs),
       (SELECT count() FROM (SELECT * FROM prop_nopk_types_small WHERE wfs = '042') AS s
        INNER JOIN prop_nopk_types_big AS b ON s.wfs = b.wfs),
       (SELECT count() FROM (SELECT * FROM prop_nopk_types_small WHERE n = 42) AS s
        INNER JOIN prop_nopk_types_big AS b ON s.n = b.n);

DROP TABLE prop_nopk_types_big;
DROP TABLE prop_nopk_types_small;
