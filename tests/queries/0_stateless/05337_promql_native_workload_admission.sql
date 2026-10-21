-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test checks local native plan admission.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables need a local database.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS promql_native_workload_admission;
CREATE TABLE promql_native_workload_admission ENGINE = TimeSeries
SETTINGS samples_index_granularity = 1;

INSERT INTO promql_native_workload_admission (metric_name, tags, samples) VALUES
    ('requests', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime64(80, 3), 0.), (toDateTime64(90, 3), 10.),
         (toDateTime64(100, 3), 20.), (toDateTime64(110, 3), 30.),
         (toDateTime64(120, 3), 40.), (toDateTime64(130, 3), 50.)]),
    ('requests', map('namespace', 'a', 'instance', 'two'),
        [(toDateTime64(80, 3), 0.), (toDateTime64(90, 3), 20.),
         (toDateTime64(100, 3), 40.), (toDateTime64(110, 3), 60.),
         (toDateTime64(120, 3), 80.), (toDateTime64(130, 3), 100.)]),
    ('requests', map('namespace', 'a', 'instance', 'three'),
        [(toDateTime64(80, 3), 0.), (toDateTime64(90, 3), 30.),
         (toDateTime64(100, 3), 60.), (toDateTime64(110, 3), 90.),
         (toDateTime64(120, 3), 120.), (toDateTime64(130, 3), 150.)]);

SET enable_promql_native_plan = 0;
CREATE TEMPORARY TABLE admission_sql AS
SELECT 'rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'rate(requests[20])', 110, 130, 10)
UNION ALL
SELECT 'sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
UNION ALL
SELECT 'hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'clamp_max(sum by (namespace) (rate(requests[20])), 2)', 110, 130, 10);

-- Exact boundaries: three physical identifiers times three evaluation points.
SET enable_promql_native_plan = 1;
SET max_promql_native_rate_series = 3;
SET max_promql_native_vector_grid_cells = 9;

SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT * FROM prometheusQueryRange(
        promql_native_workload_admission, 'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
);

CREATE TEMPORARY TABLE admission_exact AS
SELECT 'rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'rate(requests[20])', 110, 130, 10)
UNION ALL
SELECT 'sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
UNION ALL
SELECT 'hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'clamp_max(sum by (namespace) (rate(requests[20])), 2)', 110, 130, 10);

SET max_promql_native_rate_series = 2;
SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT * FROM prometheusQueryRange(
        promql_native_workload_admission, 'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
);

CREATE TEMPORARY TABLE admission_series_declined AS
SELECT 'rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'rate(requests[20])', 110, 130, 10)
UNION ALL
SELECT 'sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
UNION ALL
SELECT 'hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'clamp_max(sum by (namespace) (rate(requests[20])), 2)', 110, 130, 10);

SET max_promql_native_rate_series = 3;
SET max_promql_native_vector_grid_cells = 8;
CREATE TEMPORARY TABLE admission_grid_declined AS
SELECT 'rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'rate(requests[20])', 110, 130, 10)
UNION ALL
SELECT 'sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'sum by (namespace) (rate(requests[20]))', 110, 130, 10)
UNION ALL
SELECT 'hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_native_workload_admission, 'clamp_max(sum by (namespace) (rate(requests[20])), 2)', 110, 130, 10);

-- Compare multisets in both directions, including labels and timestamped values.
SELECT count() FROM
(
    SELECT * FROM admission_exact EXCEPT ALL SELECT * FROM admission_sql
);
SELECT count() FROM
(
    SELECT * FROM admission_sql EXCEPT ALL SELECT * FROM admission_exact
);
SELECT count() FROM
(
    SELECT * FROM admission_series_declined EXCEPT ALL SELECT * FROM admission_sql
);
SELECT count() FROM
(
    SELECT * FROM admission_sql EXCEPT ALL SELECT * FROM admission_series_declined
);
SELECT count() FROM
(
    SELECT * FROM admission_grid_declined EXCEPT ALL SELECT * FROM admission_sql
);
SELECT count() FROM
(
    SELECT * FROM admission_sql EXCEPT ALL SELECT * FROM admission_grid_declined
);

DROP TABLE promql_native_workload_admission;
