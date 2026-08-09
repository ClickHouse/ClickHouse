-- Aggregate states must not be created on the partial-GROUP-BY-key path of aggregation in order,
-- where nothing consumes them. quantileDD is used because its state owns heap allocations, so a
-- leaked state is visible to LeakSanitizer; the query result itself cannot observe the leak.

DROP TABLE IF EXISTS data_04834;

CREATE TABLE data_04834 (parent_key Int, child_key Int, value Float64)
    ENGINE = MergeTree() ORDER BY parent_key;

INSERT INTO data_04834 SELECT number % 10, number % 3, number FROM numbers(1000);

-- The leak only happens when the sorting prefix is shorter than the GROUP BY key, which is what
-- puts AggregatingInOrderTransform in `group_by_key` mode. Fail loudly if a planner change stops
-- exercising it, otherwise this test silently stops covering the bug.
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE
    SELECT parent_key, child_key, quantileDD(0.01, 0.5)(value)
    FROM data_04834 GROUP BY parent_key, child_key
    SETTINGS max_threads = 1, optimize_aggregation_in_order = 1
) WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT parent_key, child_key, round(quantileDD(0.01, 0.5)(value), 2)
FROM data_04834 GROUP BY parent_key, child_key
ORDER BY parent_key, child_key
SETTINGS max_threads = 1, optimize_aggregation_in_order = 1;

DROP TABLE data_04834;
