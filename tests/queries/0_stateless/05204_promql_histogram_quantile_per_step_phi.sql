-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- histogram_quantile accepts a phi which is not constant: each time step uses its own phi.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- Histogram `req` has job a from 100 to 140 and job b from 120; counter histogram `lat` grows from 100 to 150.
-- The gauge `phi` goes 0.5, 0.9, 0.25, 1.5, NaN, -1 at 100, 110, ..., 150.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('req_bucket', map('job', 'a', 'le', '0.1'), [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 5), (toDateTime64(130, 3), 0), (toDateTime64(140, 3), 30)]),
    ('req_bucket', map('job', 'a', 'le', '0.5'), [(toDateTime64(100, 3), 30), (toDateTime64(110, 3), 40), (toDateTime64(120, 3), 25), (toDateTime64(130, 3), 10), (toDateTime64(140, 3), 50)]),
    ('req_bucket', map('job', 'a', 'le', '1'), [(toDateTime64(100, 3), 60), (toDateTime64(110, 3), 70), (toDateTime64(120, 3), 50), (toDateTime64(130, 3), 40), (toDateTime64(140, 3), 80)]),
    ('req_bucket', map('job', 'a', 'le', '5'), [(toDateTime64(100, 3), 90), (toDateTime64(110, 3), 95), (toDateTime64(120, 3), 80), (toDateTime64(130, 3), 70), (toDateTime64(140, 3), 99)]),
    ('req_bucket', map('job', 'a', 'le', '+Inf'), [(toDateTime64(100, 3), 100), (toDateTime64(110, 3), 100), (toDateTime64(120, 3), 100), (toDateTime64(130, 3), 100), (toDateTime64(140, 3), 100)]),
    ('req_bucket', map('job', 'b', 'le', '1'), [(toDateTime64(120, 3), 1), (toDateTime64(130, 3), 2), (toDateTime64(140, 3), 4)]),
    ('req_bucket', map('job', 'b', 'le', '10'), [(toDateTime64(120, 3), 3), (toDateTime64(130, 3), 6), (toDateTime64(140, 3), 8)]),
    ('req_bucket', map('job', 'b', 'le', '+Inf'), [(toDateTime64(120, 3), 4), (toDateTime64(130, 3), 8), (toDateTime64(140, 3), 8)]),
    ('lat_bucket', map('le', '1'), arrayMap(i -> (toDateTime64(100 + i * 10, 3), i * 10.), range(6))),
    ('lat_bucket', map('le', '5'), arrayMap(i -> (toDateTime64(100 + i * 10, 3), i * 30.), range(6))),
    ('lat_bucket', map('le', '10'), arrayMap(i -> (toDateTime64(100 + i * 10, 3), i * 38.), range(6))),
    ('lat_bucket', map('le', '+Inf'), arrayMap(i -> (toDateTime64(100 + i * 10, 3), i * 40.), range(6))),
    ('phi', map(), [(toDateTime64(100, 3), 0.5), (toDateTime64(110, 3), 0.9), (toDateTime64(120, 3), 0.25), (toDateTime64(130, 3), 1.5), (toDateTime64(140, 3), nan), (toDateTime64(150, 3), -1)]);

SELECT 'range query with phi = scalar(phi)';
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'histogram_quantile(scalar(phi), req_bucket)', 100, 150, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;

SELECT 'the same phi as a constant at each step';
SELECT tags, timestamp, value FROM
(
    SELECT * FROM prometheusQuery(ts, 'histogram_quantile(0.5, req_bucket)', 100)
    UNION ALL SELECT * FROM prometheusQuery(ts, 'histogram_quantile(0.9, req_bucket)', 110)
    UNION ALL SELECT * FROM prometheusQuery(ts, 'histogram_quantile(0.25, req_bucket)', 120)
    UNION ALL SELECT * FROM prometheusQuery(ts, 'histogram_quantile(1.5, req_bucket)', 130)
    UNION ALL SELECT * FROM prometheusQuery(ts, 'histogram_quantile(NaN, req_bucket)', 140)
    UNION ALL SELECT * FROM prometheusQuery(ts, 'histogram_quantile(-1, req_bucket)', 150)
)
ORDER BY tags, timestamp;

SELECT 'range query with phi = time() / 200';
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'histogram_quantile(time() / 200, req_bucket)', 100, 140, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;

SELECT 'instant query with phi = scalar(phi)';
SELECT tags, timestamp, value FROM prometheusQuery(ts, 'histogram_quantile(scalar(phi), req_bucket)', 110) ORDER BY tags;
SELECT tags, timestamp, value FROM prometheusQuery(ts, 'histogram_quantile(scalar(phi), req_bucket)', 130) ORDER BY tags;

SELECT 'range query with a phi which is the same at each step: scalar(phi @ 110) and scalar(phi @ 150)';
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'histogram_quantile(scalar(phi @ 110), req_bucket)', 100, 150, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'histogram_quantile(scalar(phi @ 150), req_bucket)', 120, 150, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;

SELECT 'per-step phi over req_bucket @ 120';
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'histogram_quantile(scalar(phi), req_bucket @ 120)', 100, 150, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;

SELECT 'per-step phi over rate()';
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'histogram_quantile(scalar(phi), rate(lat_bucket[25s]))', 110, 150, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;

SELECT 'per-step phi inside a subquery';
SELECT tags, p.1, p.2 FROM prometheusQueryRange(ts, 'max_over_time(histogram_quantile(scalar(phi), req_bucket{job="a"})[20s:10s])', 110, 130, 10) ARRAY JOIN samples AS p ORDER BY tags, p.1;

DROP TABLE ts;
