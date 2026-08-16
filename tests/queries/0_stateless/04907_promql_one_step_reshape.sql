-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Result parity for the one-step PromQL-to-SQL reshape: single-CTE aggregation, one-step `unless`
-- lowered to an anti-join, and one-step filtering comparisons dropping rows. On a single-step grid
-- an all-null row and an absent row are indistinguishable to downstream operators, so the reshaped
-- lowering must return the same values as the multi-step lowering. The range queries at the end
-- exercise the multi-step paths, which are unchanged.

DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;
DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE prometheus ENGINE = TimeSeries
SAMPLES samples_table TAGS tags_table;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('dc', 'a', 'host', 'h1'), [(toDateTime64(100, 3), 1)]),
    ('m', map('dc', 'a', 'host', 'h2'), [(toDateTime64(100, 3), 2)]),
    ('m', map('dc', 'b', 'host', 'h3'), [(toDateTime64(100, 3), 3)]),
    ('c', map('k', 'k1'), [(toDateTime64(100, 3), 0)]),
    ('c', map('k', 'k2'), [(toDateTime64(100, 3), 5)]),
    ('c', map('k', 'k3'), [(toDateTime64(100, 3), -2)]),
    ('lm', map('p', 'p1'), [(toDateTime64(100, 3), 5)]),
    ('lm', map('p', 'p2'), [(toDateTime64(100, 3), 6)]),
    ('lm', map('p', 'p3'), [(toDateTime64(100, 3), 7)]),
    ('rm', map('p', 'p2'), [(toDateTime64(100, 3), 9)]),
    ('rm', map('p', 'p4'), [(toDateTime64(100, 3), 1)]),
    ('pending',   map('ns', 'x', 'pod', 'p1'), [(toDateTime64(100, 3), 1)]),
    ('pending',   map('ns', 'x', 'pod', 'p2'), [(toDateTime64(100, 3), 1)]),
    ('succeeded', map('ns', 'x', 'pod', 'p2'), [(toDateTime64(100, 3), 1)]);

-- Single-CTE aggregation (fused rename): sum/max/count/group/quantile by a subset of tags.
SELECT '-- sum by (dc) (m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum by (dc) (m)', 100) ORDER BY tags;
SELECT '-- max by (dc) (m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'max by (dc) (m)', 100) ORDER BY tags;
SELECT '-- count by (dc) (m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'count by (dc) (m)', 100) ORDER BY tags;
SELECT '-- group by (dc) (m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'group by (dc) (m)', 100) ORDER BY tags;
SELECT '-- quantile by (dc) (0.5, m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile by (dc) (0.5, m)', 100) ORDER BY tags;
SELECT '-- sum without (host) (m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum without (host) (m)', 100) ORDER BY tags;
SELECT '-- bare sum(m)';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum(m)', 100) ORDER BY tags;

-- One-step filtering comparisons: rows that fail the predicate are dropped, not kept as NULL.
SELECT '-- c > 0';
SELECT tags, value FROM prometheusQuery('prometheus', 'c > 0', 100) ORDER BY tags;
SELECT '-- c == 5';
SELECT tags, value FROM prometheusQuery('prometheus', 'c == 5', 100) ORDER BY tags;
SELECT '-- c != 0';
SELECT tags, value FROM prometheusQuery('prometheus', 'c != 0', 100) ORDER BY tags;
SELECT '-- c <= -2';
SELECT tags, value FROM prometheusQuery('prometheus', 'c <= -2', 100) ORDER BY tags;
SELECT '-- 0 < c (scalar on the left)';
SELECT tags, value FROM prometheusQuery('prometheus', '0 < c', 100) ORDER BY tags;
SELECT '-- c > 100 (all rows dropped, empty result)';
SELECT tags, value FROM prometheusQuery('prometheus', 'c > 100', 100) ORDER BY tags;

-- One-step `unless` lowered to an anti-join.
SELECT '-- lm unless on (p) rm';
SELECT tags, value FROM prometheusQuery('prometheus', 'lm unless on (p) rm', 100) ORDER BY tags;
SELECT '-- lm unless on (p) (rm > 100) : right side empty, nothing removed';
SELECT tags, value FROM prometheusQuery('prometheus', 'lm unless on (p) (rm > 100)', 100) ORDER BY tags;

-- Composite alert shape: aggregation + filter + unless together.
SELECT '-- sum by (ns, pod) (max by (ns, pod) (pending)) > 0 unless on (ns, pod) (succeeded == 1)';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum by (ns, pod) (max by (ns, pod) (pending)) > 0 unless on (ns, pod) (succeeded == 1)', 100) ORDER BY tags;

-- Multi-step range queries exercise the unchanged aggregation, comparison, and `unless` paths.
-- prometheusQueryRange returns (tags, time_series), not (tags, value).
SELECT '-- range: sum by (dc) (m)';
SELECT * FROM prometheusQueryRange('prometheus', 'sum by (dc) (m)', 100, 120, 10) ORDER BY tags;
SELECT '-- range: c > 0';
SELECT * FROM prometheusQueryRange('prometheus', 'c > 0', 100, 120, 10) ORDER BY tags;
SELECT '-- range: lm unless on (p) rm';
SELECT * FROM prometheusQueryRange('prometheus', 'lm unless on (p) rm', 100, 120, 10) ORDER BY tags;

DROP TABLE prometheus;
DROP TABLE samples_table;
DROP TABLE tags_table;
