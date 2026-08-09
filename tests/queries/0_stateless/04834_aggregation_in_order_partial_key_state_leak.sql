-- Aggregate states must not be created on the partial-GROUP-BY-key path of aggregation in order,
-- where nothing consumes them. quantileDD is used because its state owns heap allocations, so a
-- leaked state is visible to LeakSanitizer; the query result itself cannot observe the leak.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS data_04834;

CREATE TABLE data_04834 (parent_key Int, child_key Int, value Float64)
    ENGINE = MergeTree() ORDER BY parent_key;

INSERT INTO data_04834 SELECT number % 10, number % 3, number FROM numbers(1000);

-- The leak needs the sorting prefix to be SHORTER than the GROUP BY key, which is what selects
-- `group_by_key` mode. Assert the prefix itself: the processor name alone is identical in both
-- modes, so it cannot tell them apart. Parallel replicas add a second `Order:` line from the
-- coordinator's MergingAggregated step, so pin the setting the assertion depends on.
SELECT trimBoth(replaceRegexpAll(explain, '__table1.', ''))
FROM (
    EXPLAIN actions = 1
    SELECT parent_key, child_key, quantileDD(0.01, 0.5)(value)
    FROM data_04834 GROUP BY parent_key, child_key
    SETTINGS max_threads = 1, optimize_aggregation_in_order = 1, enable_parallel_replicas = 0
)
WHERE explain LIKE '%Order:%';

SELECT parent_key, child_key, round(quantileDD(0.01, 0.5)(value), 2)
FROM data_04834 GROUP BY parent_key, child_key
ORDER BY parent_key, child_key
SETTINGS max_threads = 1, optimize_aggregation_in_order = 1;

DROP TABLE data_04834;
