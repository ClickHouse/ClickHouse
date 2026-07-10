-- Tags: no-replicated-database
-- no-replicated-database: SYSTEM STOP MERGES works only on one replica

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS gate_mut_src;
DROP TABLE IF EXISTS gate_mut_skip;

CREATE TABLE gate_mut_src (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
-- k is covered only by a minmax skipping index
CREATE TABLE gate_mut_skip (k UInt64, payload String, INDEX ix_k k TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY payload
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO gate_mut_src  SELECT number, toString(number) FROM numbers(100000);
INSERT INTO gate_mut_skip SELECT number, toString(number) FROM numbers(100000);

-- Counting the predicate in filter steps: 1 = source side only, 2 = lifted to target too

-- Before the update the skip index is usable, the lift fires
SELECT 'no pending update',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_mut_src WHERE k = 12345) AS o
    INNER JOIN gate_mut_skip AS l ON o.k = l.k
);

-- Keep the patch parts unmaterialized so the update stays applied on the fly
SYSTEM STOP MERGES gate_mut_skip;
UPDATE gate_mut_skip SET k = k WHERE 1;

-- Every part now has an on-fly update of `k`, so the read path rejects `ix_k` per part
-- and the lifted copy could not prune anything: the gate must bail
SELECT 'pending update on index column',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM gate_mut_src WHERE k = 12345) AS o
    INNER JOIN gate_mut_skip AS l ON o.k = l.k
);

-- Same result with and without the lift while the update is pending
SELECT 'pending update correctness',
       (SELECT count() FROM (SELECT * FROM gate_mut_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_mut_skip AS l ON o.k = l.k)
     - (SELECT count() FROM (SELECT * FROM gate_mut_src WHERE k BETWEEN 100 AND 200) AS o
        INNER JOIN gate_mut_skip AS l ON o.k = l.k
        SETTINGS query_plan_lift_predicate_across_join = 0);

SYSTEM START MERGES gate_mut_skip;

DROP TABLE gate_mut_src;
DROP TABLE gate_mut_skip;
