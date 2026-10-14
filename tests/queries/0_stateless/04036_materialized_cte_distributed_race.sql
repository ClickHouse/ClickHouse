SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- Reproduce LOGICAL_ERROR: "Reading from materialized CTE before it has been materialized"
-- The bug: when a materialized CTE is used with a Distributed table, buildQueryTreeForShard
-- eagerly reads from the CTE's StorageMemory during plan building, before
-- MaterializingCTETransform has populated it.
-- The CTE must be referenced multiple times to prevent inlining.

CREATE TABLE IF NOT EXISTS local_04036 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE IF NOT EXISTS dist_04036 AS local_04036 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_04036);

INSERT INTO local_04036 SELECT number FROM numbers(100);

-- Materialized CTE referenced twice via short IN form with Distributed table.
-- buildQueryTreeForShard encounters the TableNode in IN and tries to eagerly execute it.
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM dist_04036
WHERE x IN (t) OR x IN (t);

-- Same with subquery form
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM dist_04036
WHERE x IN (SELECT c FROM t WHERE c < 10) OR x IN (SELECT c FROM t WHERE c >= 10 AND c < 50);

-- Materialized CTE in JOIN + IN with Distributed table
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM dist_04036
LEFT JOIN t ON dist_04036.x = t.c
WHERE dist_04036.x IN t;

DROP TABLE IF EXISTS dist_04036;
DROP TABLE IF EXISTS local_04036;
