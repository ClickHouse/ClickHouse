-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

-- GROUP BY raw-id paths: rate + sum by + vector matching (group_left/unless), @ modifier, top-level range selector, offset error path, duplicated tag sets, instant and range queries.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_data;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_metrics;

CREATE TABLE ts_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_tags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
-- `tags` is functionally dependent on `id`, so it is kept outside the sorting key on purpose.
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts_metrics (
    metric_family_name String,
    type String,
    unit String,
    help String)
ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts ENGINE = TimeSeries
DATA ts_data TAGS ts_tags METRICS ts_metrics;

-- Three counter series of `http_requests_total` (two namespaces) and one `limit_per_ns` gauge series per namespace.
INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    ('00000000-0000-0000-0000-000000000001', 'http_requests_total', map('ns', 'a', 'pod', 'p1'), toDateTime64(0, 3, 'UTC'), toDateTime64(300, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000002', 'http_requests_total', map('ns', 'a', 'pod', 'p2'), toDateTime64(0, 3, 'UTC'), toDateTime64(300, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000003', 'http_requests_total', map('ns', 'b', 'pod', 'p1'), toDateTime64(0, 3, 'UTC'), toDateTime64(300, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000011', 'limit_per_ns', map('ns', 'a'), toDateTime64(0, 3, 'UTC'), toDateTime64(300, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000012', 'limit_per_ns', map('ns', 'b'), toDateTime64(0, 3, 'UTC'), toDateTime64(300, 3, 'UTC'));

-- Counters grow linearly with slopes 1, 2 and 3 per second, one sample every 30 seconds over [0, 300].
INSERT INTO ts_data
SELECT s.1::UUID AS id, toDateTime64(number * 30, 3, 'UTC') AS timestamp, s.2 * number * 30 AS value
FROM numbers(11)
ARRAY JOIN
    [('00000000-0000-0000-0000-000000000001', 1.),
     ('00000000-0000-0000-0000-000000000002', 2.),
     ('00000000-0000-0000-0000-000000000003', 3.)] AS s;

-- The gauges are constant: 100 for ns=a, 200 for ns=b.
INSERT INTO ts_data
SELECT s.1::UUID AS id, toDateTime64(number * 30, 3, 'UTC') AS timestamp, s.2 AS value
FROM numbers(11)
ARRAY JOIN
    [('00000000-0000-0000-0000-000000000011', 100.),
     ('00000000-0000-0000-0000-000000000012', 200.)] AS s;

SELECT '-- instant selector';
SELECT * FROM prometheusQuery('ts', 'http_requests_total', 240) ORDER BY tags;

SELECT '-- rate';
SELECT * FROM prometheusQuery('ts', 'rate(http_requests_total[60s])', 240) ORDER BY tags;

SELECT '-- sum by rate';
SELECT * FROM prometheusQuery('ts', 'sum by (ns) (rate(http_requests_total[60s]))', 240) ORDER BY tags;

SELECT '-- group_left join';
SELECT * FROM prometheusQuery('ts', 'rate(http_requests_total[60s]) * on (ns) group_left limit_per_ns', 240) ORDER BY tags;

SELECT '-- unless';
SELECT * FROM prometheusQuery('ts', 'http_requests_total unless on (ns) limit_per_ns{ns="a"}', 240) ORDER BY tags;

SELECT '-- rate over a range selector with the @ modifier';
-- The result is empty: the @ modifier snaps all raw timestamps to the evaluation grid, so no rate can be computed, but the query must not fail.
SELECT * FROM prometheusQuery('ts', 'rate(http_requests_total[60s] @ 240)', 300) ORDER BY tags;

SELECT '-- last_over_time over a range selector with the @ modifier';
-- Exactly one sample (t=240) falls into the window [215, 240], so this value-checks the id/timestamp/value plumbing of the @ modifier path.
SELECT * FROM prometheusQuery('ts', 'last_over_time(http_requests_total[25s] @ 240)', 300) ORDER BY tags;

SELECT '-- offset on a range selector';
-- offset on a range selector fails due to a pre-existing applyOffset scale-rounding bug (std::max vs std::min on the 9 cap); this pins the error, update the reference once fixed.
SELECT * FROM prometheusQuery('ts', 'rate(http_requests_total[60s] offset 30s)', 240); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- top-level range selector';
SELECT * FROM prometheusQuery('ts', 'http_requests_total[90s]', 240) ORDER BY tags;

SELECT '-- range query: sum by rate';
SELECT * FROM prometheusQueryRange('ts', 'sum by (ns) (rate(http_requests_total[60s]))', 120, 300, 60) ORDER BY tags;

SELECT '-- range query: rate with group_left join';
SELECT * FROM prometheusQueryRange('ts', 'rate(http_requests_total[60s]) * on (ns) group_left limit_per_ns', 120, 300, 90) ORDER BY tags;

-- Two ids sharing one tag set (only constructible via manual inner-table inserts) pin raw-id grouping: selectors emit one row per id, rate() rejects the duplicate series.
INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    ('00000000-0000-0000-0000-000000000009', 'http_requests_total', map('ns', 'a', 'pod', 'p1'), toDateTime64(0, 3, 'UTC'), toDateTime64(300, 3, 'UTC'));
INSERT INTO ts_data SELECT '00000000-0000-0000-0000-000000000009'::UUID, toDateTime64(number * 30, 3, 'UTC'), 1000. FROM numbers(11);

SELECT '-- duplicated tag set: instant selector emits one row per id';
SELECT * FROM prometheusQuery('ts', 'http_requests_total{ns="a", pod="p1"}', 240) ORDER BY tags, value;

SELECT '-- duplicated tag set: top-level range selector emits one row per id';
SELECT * FROM prometheusQuery('ts', 'http_requests_total{ns="a", pod="p1"}[90s]', 240) ORDER BY tags, time_series;

SELECT '-- duplicated tag set: rate() rejects duplicate series';
SELECT * FROM prometheusQuery('ts', 'rate(http_requests_total{ns="a", pod="p1"}[60s])', 240); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_data;
