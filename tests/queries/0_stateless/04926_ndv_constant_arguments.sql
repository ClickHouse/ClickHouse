-- Check NDV propagation through deterministic multi-argument functions with exactly one non-constant argument:
--   `toUInt64(dateTrunc('month', d))`
--   `plus(plus(materialize(1), materialize(1)), n)`
--   `toUInt64(dateTrunc((SELECT 'month'), d))`

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

-- Check NDV propagation with a literal constant: `toUInt64(dateTrunc('month', d))`.
-- `NDV(d) = NDV(n) = 10` -> estimated groups: `aggregated[10]` in `EXPLAIN`.
SELECT 'toUInt64(dateTrunc(\'month\', d))';
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

-- Check NDV propagation with shared `materialize(1)`: `plus(plus(materialize(1), materialize(1)), n)`.
-- The function call `materialize(1)` produces a regular column `[1, 1, ...]`.
SELECT 'plus(plus(materialize(1), materialize(1)), n)';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM probe
    JOIN
    (
        SELECT plus(plus(materialize(1), materialize(1)), n) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated
    ON probe.n = aggregated.key
)
WHERE explain LIKE '% Join:%';

-- Check NDV propagation with a constant from a scalar subquery: `toUInt64(dateTrunc((SELECT 'month'), d))`.
SELECT 'toUInt64(dateTrunc((SELECT \'month\'), d))';
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
