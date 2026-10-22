SET enable_analyzer = 1, enable_materialized_cte = 1;
SET serialize_query_plan = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS pr_cte_04227;
CREATE TABLE pr_cte_04227 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO pr_cte_04227 SELECT * FROM numbers(1000);

-- Two IN-subqueries on the primary key, both reading from the same materialized CTE.
-- A single reference would let `inlineMaterializedCTEIfNeeded` inline the CTE and bypass
-- the materialized-CTE machinery; two references keep it materialized and pull both sets
-- into primary-key analysis, which routes through `addBuildSubqueriesForSetsStepIfNeeded`
-- and exercises the remote-plan builder for materialized CTEs.
--
-- This shape previously crashed with `LOGICAL_ERROR: CTE 't' does not have query tree,
-- but was not planned yet` under `serialize_query_plan = 1` + parallel replicas.
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT count() FROM pr_cte_04227
WHERE x IN (SELECT c FROM t) OR x IN (SELECT c FROM t);

DROP TABLE pr_cte_04227;
