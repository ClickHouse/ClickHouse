CREATE TABLE source (n UInt64, d Date) ENGINE = MergeTree ORDER BY n SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE probe (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;

INSERT INTO source SELECT number % 10, toDate('2020-01-01') + number % 10 FROM numbers(100);
INSERT INTO probe SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET use_statistics = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;

SELECT 'constant argument before the non-const argument';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM probe
    JOIN
    (
        SELECT toUInt64(dateTrunc('month', d)) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated
    ON probe.n = aggregated.key
)
WHERE explain LIKE '% Join:%';

SELECT 'non-folded constant argument';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM probe
    JOIN
    (
        SELECT plus(materialize(1), n) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated
    ON probe.n = aggregated.key
)
WHERE explain LIKE '% Join:%';

SELECT 'constant scalar subquery argument';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM probe
    JOIN
    (
        SELECT toUInt64(dateTrunc((SELECT 'month'), d)) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated
    ON probe.n = aggregated.key
)
WHERE explain LIKE '% Join:%';
