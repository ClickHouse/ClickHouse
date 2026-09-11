-- Check NDV and width propagation through:
--   `concat(materialize(repeat('x', 1000)), s)`
--   `substring(s, 2)`
--   `materialize(s)`
--   `identity(s)`

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_cascades_optimizer = 0;
SET make_distributed_plan = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;

CREATE TABLE source (s String) ENGINE = MergeTree ORDER BY s SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE probe (s String) ENGINE = MergeTree ORDER BY s SETTINGS auto_statistics_types = 'uniq';
INSERT INTO source SELECT leftPad(toString(number % 100), 10, 'x') FROM numbers(1000);
INSERT INTO probe SELECT leftPad(toString(number), 10, 'x') FROM numbers(100);

-- `NDV(s) = 100` -> estimated groups: `aggregated[100]` in `EXPLAIN`.
-- Check NDV propagation through `concat(materialize(repeat('x', 1000)), s)`.
SELECT 'NDV: concat(materialize(repeat(\'x\', 1000)), s)';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM probe JOIN
    (
        SELECT concat(materialize(repeat('x', 1000)), s) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated ON probe.s = aggregated.key
)
WHERE explain LIKE '% Join:%';

-- Check NDV propagation through `substring(s, 2)`.
SELECT 'NDV: substring(s, 2)';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM probe JOIN
    (
        SELECT substring(s, 2) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated ON probe.s = aggregated.key
)
WHERE explain LIKE '% Join:%';

-- Check NDV propagation through `materialize(s)`.
SELECT 'NDV: materialize(s)';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM probe JOIN
    (
        SELECT materialize(s) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated ON probe.s = aggregated.key
)
WHERE explain LIKE '% Join:%';

-- Check NDV propagation through `identity(s)`.
SELECT 'NDV: identity(s)';
SELECT extract(explain, 'Join:.*') FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM probe JOIN
    (
        SELECT identity(s) AS key, count()
        FROM source
        GROUP BY key
    ) AS aggregated ON probe.s = aggregated.key
)
WHERE explain LIKE '% Join:%';

DROP TABLE source;
DROP TABLE probe;

-- Pin row counts, column widths, and part layout for the width-dependent plan checks.
-- A 10-byte `s` favors shuffling 5M joined rows; the 64-byte `String` default favors shuffling 10M fact rows.
SET max_threads = 4;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"fact": {"cardinality": 10000000, "avg_row_bytes": 24, "distinct_keys": {"k": 1000000, "g": 10}, "column_bytes": {"k": 8, "g": 8}}, "dim": {"cardinality": 5, "avg_row_bytes": 18, "distinct_keys": {"g": 5}, "column_bytes": {"g": 8, "s": 10}}}';

CREATE TABLE fact (k UInt64, g UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS auto_statistics_types = '', index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE dim (g UInt64, s String) ENGINE = MergeTree ORDER BY g
    SETTINGS auto_statistics_types = '', index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO fact SELECT number, number % 10, number FROM numbers(1000) SETTINGS max_insert_threads = 1;
INSERT INTO dim SELECT number, leftPad(toString(number), 10, 'x') FROM numbers(5) SETTINGS max_insert_threads = 1;

-- Check width propagation through `concat(materialize(repeat('x', 1000)), s)`: use the 64-byte default; shuffle below the join.
SELECT 'width: concat(materialize(repeat(\'x\', 1000)), s)',
       countIf(explain LIKE '%ShuffleExchange%') = 1
       AND countIf(explain LIKE '%Broadcast HashJoin%') = 1
       AND minIf(rn, explain LIKE '%ShuffleExchange%') > minIf(rn, explain LIKE '%Broadcast HashJoin%')
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn FROM
    (
        EXPLAIN
        SELECT k, sum(v), any(s)
        FROM fact AS f
        JOIN (SELECT g, concat(materialize(repeat('x', 1000)), s) AS s FROM dim) AS d ON f.g = d.g
        GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
                 distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
    )
);

-- Check width propagation through `substring(s, 2)`: use the 64-byte default; shuffle below the join.
SELECT 'width: substring(s, 2)',
       countIf(explain LIKE '%ShuffleExchange%') = 1
       AND countIf(explain LIKE '%Broadcast HashJoin%') = 1
       AND minIf(rn, explain LIKE '%ShuffleExchange%') > minIf(rn, explain LIKE '%Broadcast HashJoin%')
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn FROM
    (
        EXPLAIN
        SELECT k, sum(v), any(s)
        FROM fact AS f
        JOIN (SELECT g, substring(s, 2) AS s FROM dim) AS d ON f.g = d.g
        GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
                 distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
    )
);

-- Check width propagation through `materialize(s)`: keep 10 bytes; shuffle above the join.
SELECT 'width: materialize(s)',
       countIf(explain LIKE '%ShuffleExchange%') = 1
       AND countIf(explain LIKE '%Broadcast HashJoin%') = 1
       AND minIf(rn, explain LIKE '%ShuffleExchange%') < minIf(rn, explain LIKE '%Broadcast HashJoin%')
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn FROM
    (
        EXPLAIN
        SELECT k, sum(v), any(s)
        FROM fact AS f
        JOIN (SELECT g, materialize(s) AS s FROM dim) AS d ON f.g = d.g
        GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
                 distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
    )
);

-- Check width propagation through `identity(s)`: keep 10 bytes; shuffle above the join.
SELECT 'width: identity(s)',
       countIf(explain LIKE '%ShuffleExchange%') = 1
       AND countIf(explain LIKE '%Broadcast HashJoin%') = 1
       AND minIf(rn, explain LIKE '%ShuffleExchange%') < minIf(rn, explain LIKE '%Broadcast HashJoin%')
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn FROM
    (
        EXPLAIN
        SELECT k, sum(v), any(s)
        FROM fact AS f
        JOIN (SELECT g, identity(s) AS s FROM dim) AS d ON f.g = d.g
        GROUP BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
                 distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
    )
);

DROP TABLE fact;
DROP TABLE dim;
