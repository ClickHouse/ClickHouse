-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test checks which local plan step evaluates PromQL.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS promql_native_grid_cell_admission;
CREATE TABLE promql_native_grid_cell_admission ENGINE = TimeSeries
SETTINGS samples_index_granularity = 1;

INSERT INTO promql_native_grid_cell_admission (metric_name, tags, samples) VALUES
    ('requests', map('namespace', 'a'),
        [(toDateTime64(80, 3), 0.), (toDateTime64(90, 3), 10.),
         (toDateTime64(100, 3), 20.), (toDateTime64(110, 3), 30.),
         (toDateTime64(120, 3), 40.), (toDateTime64(130, 3), 50.)]);

-- One physical series and three evaluation points need three grid cells.
-- A one-cell budget must decline native admission for direct and hybrid paths.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT * FROM prometheusQueryRange(
        promql_native_grid_cell_admission,
        'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 1
);

SELECT countIf(explain LIKE '%PromQLRangeRate%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(
        promql_native_grid_cell_admission,
        'rate(requests[20])', 110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 1
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT * FROM prometheusQueryRange(
        promql_native_grid_cell_admission,
        'clamp_max(sum by (namespace) (rate(requests[20])), 2)', 110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 1
);

-- A sufficient budget preserves native admission.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT * FROM prometheusQueryRange(
        promql_native_grid_cell_admission,
        'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 3
);

SELECT countIf(explain LIKE '%PromQLRangeRate%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(
        promql_native_grid_cell_admission,
        'rate(requests[20])', 110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 3
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT * FROM prometheusQueryRange(
        promql_native_grid_cell_admission,
        'clamp_max(sum by (namespace) (rate(requests[20])), 2)', 110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 3
);

CREATE TEMPORARY TABLE grid_sql AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_native_grid_cell_admission,
    'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE grid_bounded AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_native_grid_cell_admission,
    'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
SETTINGS enable_promql_native_plan = 1, max_promql_native_vector_grid_cells = 1;

SELECT count() FROM
(
    SELECT tags, samples FROM grid_bounded
    EXCEPT ALL
    SELECT tags, samples FROM grid_sql
);

SELECT count() FROM
(
    SELECT tags, samples FROM grid_sql
    EXCEPT ALL
    SELECT tags, samples FROM grid_bounded
);

DROP TABLE promql_native_grid_cell_admission;
