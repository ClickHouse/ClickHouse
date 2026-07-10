-- Tags: long
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
-- The gate judges local MergeTree reads; under parallel replicas the plan reads through
-- remote-replica steps and the gate correctly keeps the old behavior, changing EXPLAIN output
SET enable_parallel_replicas = 0;
-- The gate is opt-in: a selective inferred condition can reduce join input even without pruning
SET query_plan_filter_push_down_inferred_only_for_pruning = 1;

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

-- The WHERE condition is written on o.k only; the copy on the other side is inferred through
-- the join key equivalence. Counting the predicate in filter steps: 1 = kept on the original
-- side only, 2 = an inferred copy was added to the other side too.

-- Control: receiving side's key is its primary key, inferred copy prunes and is added
SELECT 'pk target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
    WHERE o.k = 12345
);

-- Receiving side's key is not covered by any index: the inferred copy cannot prune anything
-- and would only add a redundant per-row filter, so it is skipped
SELECT 'plain target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_plain AS l ON o.k = l.k
    WHERE o.k = 12345
);

-- With the gate disabled the redundant copy is added again (old behavior)
SELECT 'plain target, gate off',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_plain AS l ON o.k = l.k
    WHERE o.k = 12345
    SETTINGS query_plan_filter_push_down_inferred_only_for_pruning = 0
);

-- Receiving side's key is covered by the partition key: inferred copy prunes partitions
SELECT 'partition target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_part AS l ON o.k = l.k
    WHERE o.k = 12345
);

-- Receiving side's key is covered by a minmax skipping index: inferred copy prunes granules
SELECT 'skip index target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_skip AS l ON o.k = l.k
    WHERE o.k = 12345
);

-- FINAL suppresses partition pruning for this table (defer_partition_pruning_after_final),
-- so the inferred partition-key copy cannot prune and is skipped
SELECT 'final partition target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN (SELECT * FROM gate_final FINAL) AS l ON o.k = l.k
    WHERE o.k = 12345
    SETTINGS defer_partition_pruning_after_final = 1
);

-- With the FINAL suppression opted out, the same copy prunes partitions again and is added
SELECT 'final opt-out partition target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN (SELECT * FROM gate_final FINAL) AS l ON o.k = l.k
    WHERE o.k = 12345
    SETTINGS defer_partition_pruning_after_final = 0
);

-- LEFT JOIN: the left-side condition is still inferred for the right side when it prunes there
SELECT 'left join pk target',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    LEFT JOIN gate_pk AS l ON o.k = l.k
    WHERE o.k = 12345
);

-- Mixed conjunction: the pruning-capable atom is inferred for the PK side, the
-- index-unusable one (hash of the key) is not
SELECT 'mixed conjunction inferred atom',
       countIf(explain LIKE '%ilter column:%k = 42%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
    WHERE o.k = 42 AND sipHash64(o.k) % 2 >= 0
);

SELECT 'mixed conjunction skipped atom',
       countIf(explain LIKE '%ilter column:%sipHash64%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM gate_src AS o
    INNER JOIN gate_pk AS l ON o.k = l.k
    WHERE o.k = 42 AND sipHash64(o.k) % 2 >= 0
);

-- Correctness: same result with and without the gate for every outcome
SELECT 'plain correctness',
       (SELECT count() FROM gate_src AS o INNER JOIN gate_plain AS l ON o.k = l.k
        WHERE o.k BETWEEN 100 AND 200)
     - (SELECT count() FROM gate_src AS o INNER JOIN gate_plain AS l ON o.k = l.k
        WHERE o.k BETWEEN 100 AND 200
        SETTINGS query_plan_filter_push_down_inferred_only_for_pruning = 0);

SELECT 'partition correctness',
       (SELECT count() FROM gate_src AS o INNER JOIN gate_part AS l ON o.k = l.k
        WHERE o.k BETWEEN 100 AND 200)
     - (SELECT count() FROM gate_src AS o INNER JOIN gate_part AS l ON o.k = l.k
        WHERE o.k BETWEEN 100 AND 200
        SETTINGS query_plan_filter_push_down_inferred_only_for_pruning = 0);

SELECT 'skip index correctness',
       (SELECT count() FROM gate_src AS o INNER JOIN gate_skip AS l ON o.k = l.k
        WHERE o.k BETWEEN 100 AND 200)
     - (SELECT count() FROM gate_src AS o INNER JOIN gate_skip AS l ON o.k = l.k
        WHERE o.k BETWEEN 100 AND 200
        SETTINGS query_plan_filter_push_down_inferred_only_for_pruning = 0);

SELECT 'left join correctness',
       (SELECT count() FROM gate_src AS o LEFT JOIN gate_plain AS l ON o.k = l.k
        WHERE o.k BETWEEN 999990 AND 999999)
     - (SELECT count() FROM gate_src AS o LEFT JOIN gate_plain AS l ON o.k = l.k
        WHERE o.k BETWEEN 999990 AND 999999
        SETTINGS query_plan_filter_push_down_inferred_only_for_pruning = 0);

-- A conjunct mixing equivalent columns of both sides has no original on either side; its
-- substituted copies are load-bearing and must never be vetoed
SELECT 'mixed-side atom correctness',
       (SELECT count() FROM gate_src AS o INNER JOIN gate_plain AS l ON o.k = l.k
        WHERE o.k + l.k = 24690)
     - (SELECT count() FROM gate_src AS o INNER JOIN gate_plain AS l ON o.k = l.k
        WHERE o.k + l.k = 24690
        SETTINGS query_plan_filter_push_down_inferred_only_for_pruning = 0);

SELECT 'mixed-side atom result',
       count()
FROM gate_src AS o INNER JOIN gate_plain AS l ON o.k = l.k
WHERE o.k + l.k = 24690;

DROP TABLE gate_src;
DROP TABLE gate_pk;
DROP TABLE gate_plain;
DROP TABLE gate_part;
DROP TABLE gate_skip;
DROP TABLE gate_final;
