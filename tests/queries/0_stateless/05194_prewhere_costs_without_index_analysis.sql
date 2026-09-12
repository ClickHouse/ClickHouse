SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET explain_query_plan_default = 'legacy';

CREATE TABLE prewhere_costs_without_index_analysis (p UInt32, value UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO prewhere_costs_without_index_analysis SELECT number % 4, number FROM numbers(40);

-- Automatic parallel-replica candidate plans disable primary-key analysis too.
-- They must not choose `PREWHERE` costs from an unfiltered part snapshot.
SELECT countIf(explain LIKE '%Prewhere info%')
FROM
(
    EXPLAIN actions = 1
    SELECT sum(value) FROM prewhere_costs_without_index_analysis WHERE p = 3 AND value > 20
    SETTINGS query_plan_optimize_primary_key = 0
);

SELECT sum(value) FROM prewhere_costs_without_index_analysis WHERE p = 3 AND value > 20
SETTINGS query_plan_optimize_primary_key = 0;

DROP TABLE prewhere_costs_without_index_analysis;
