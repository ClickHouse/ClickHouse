-- Tags: long
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
-- The lift targets local MergeTree reads; under parallel replicas the plan reads through
-- remote-replica steps and the pass correctly bails, changing the EXPLAIN output
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS gate_src;
DROP TABLE IF EXISTS gate_pk;
DROP TABLE IF EXISTS gate_plain;
DROP TABLE IF EXISTS gate_part;
DROP TABLE IF EXISTS gate_skip;
DROP TABLE IF EXISTS gate_final;

CREATE TABLE gate_src   (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE gate_pk    (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
-- k is not covered by the primary key, the partition key, or any skipping index
CREATE TABLE gate_plain (k UInt64, payload String) ENGINE = MergeTree ORDER BY payload;
-- k is covered by the partition key only
CREATE TABLE gate_part  (k UInt64, payload String) ENGINE = MergeTree PARTITION BY intDiv(k, 250000) ORDER BY payload;
-- k is covered by a minmax skipping index only
CREATE TABLE gate_skip  (k UInt64, payload String, INDEX ix_k k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY payload;
-- k is covered by the partition key, but FINAL suppresses partition pruning here because
-- rows with the same sorting key may span partitions (partition key not from sorting key)
CREATE TABLE gate_final (k UInt64, payload String) ENGINE = ReplacingMergeTree PARTITION BY intDiv(k, 250000) ORDER BY payload;

INSERT INTO gate_src   SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO gate_pk    SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO gate_plain SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO gate_part  SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO gate_skip  SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO gate_final SELECT number, toString(number) FROM numbers(1000000);

-- Counting occurrences of the predicate in filter steps: 1 = source side only, 2 = lifted to target too

-- Control: target key is the primary key, lift fires
SELECT 'pk target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
);

-- Target key is not covered by any index: the lifted copy cannot prune anything and would
-- only add a redundant per-row filter, so the gate must bail
SELECT 'plain target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN gate_plain AS l ON o.k = l.k
);

-- Target key is covered by the partition key: lift prunes partitions
SELECT 'partition target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN gate_part AS l ON o.k = l.k
);

-- Target key is covered by a minmax skipping index: lift prunes granules
SELECT 'skip index target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN gate_skip AS l ON o.k = l.k
);

-- FINAL suppresses partition pruning for this table (defer_partition_pruning_after_final),
-- so the partition-key conjunct cannot prune and must not be lifted
SELECT 'final partition target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN (SELECT * FROM gate_final FINAL) AS l ON o.k = l.k
    SETTINGS defer_partition_pruning_after_final = 1
);

-- With the FINAL suppression opted out, the same conjunct prunes partitions again and is lifted
SELECT 'final opt-out partition target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN (SELECT * FROM gate_final FINAL) AS l ON o.k = l.k
    SETTINGS defer_partition_pruning_after_final = 0
);

-- Mixed conjunction on a PK-covered key: the pruning-capable atom is lifted, the
-- index-unusable one (hash of the key) is not
SELECT 'mixed conjunction lifted atom',
       countIf(explain LIKE '%ilter column:%k = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 42 AND sipHash64(k) % 2 >= 0) AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
);

SELECT 'mixed conjunction skipped atom',
       countIf(explain LIKE '%ilter column:%sipHash64%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 42 AND sipHash64(k) % 2 >= 0) AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
);

-- Index-unusable atom alone on a PK-covered key: nothing prunes, no lift at all
SELECT 'unusable atom only',
       countIf(explain LIKE '%Lifted equi-join filter%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE sipHash64(k) % 2 >= 0) AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
);

-- The lift depends on PK re-analysis after the pushdown; with it disabled the lifted
-- filter could never reach index analysis, so the lift must not fire even on a PK key
SELECT 'no pk analysis',
       countIf(explain LIKE '%Lifted equi-join filter%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_src WHERE k = 12345) AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
    SETTINGS query_plan_optimize_primary_key = 0
);

-- Correctness: same result with and without the lift for every gate outcome
SELECT 'plain correctness',
       (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_plain AS l ON o.k = l.k)
     - (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_plain AS l ON o.k = l.k
        SETTINGS query_plan_lift_predicate_across_join = 0);

SELECT 'partition correctness',
       (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_part AS l ON o.k = l.k)
     - (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_part AS l ON o.k = l.k
        SETTINGS query_plan_lift_predicate_across_join = 0);

SELECT 'skip index correctness',
       (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_skip AS l ON o.k = l.k)
     - (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_skip AS l ON o.k = l.k
        SETTINGS query_plan_lift_predicate_across_join = 0);

SELECT 'mixed correctness',
       (SELECT count() FROM (SELECT * FROM gate_src WHERE k = 42 AND sipHash64(k) % 2 >= 0) AS o
        INNER JOIN gate_pk AS l ON o.k = l.k)
     - (SELECT count() FROM (SELECT * FROM gate_src WHERE k = 42 AND sipHash64(k) % 2 >= 0) AS o
        INNER JOIN gate_pk AS l ON o.k = l.k
        SETTINGS query_plan_lift_predicate_across_join = 0);

SELECT 'final correctness',
       (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN (SELECT * FROM gate_final FINAL) AS l ON o.k = l.k
        SETTINGS defer_partition_pruning_after_final = 0)
     - (SELECT count() FROM (SELECT * FROM gate_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN (SELECT * FROM gate_final FINAL) AS l ON o.k = l.k
        SETTINGS query_plan_lift_predicate_across_join = 0);

DROP TABLE gate_src;
DROP TABLE gate_pk;
DROP TABLE gate_plain;
DROP TABLE gate_part;
DROP TABLE gate_skip;
DROP TABLE gate_final;
