-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A group after a literal or after another group keeps regex semantics.

DROP TABLE IF EXISTS rx;
SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx;
CREATE TABLE rx ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'myweb'}, [(1000, 2.)]),
    ('a_total', {'job':'api-server'}, [(1000, 3.)]),
    ('a_total', {'job':''}, [(1000, 4.)]),
    ('a_total', {'instance':'x'}, [(1000, 5.)]),
    ('a_total', {'job':'web'}, [(1000, 6.)]);

SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api(-server|)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job!~"api(-server|)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"(api|)(|-server)"}', 1000) ORDER BY value);

DROP TABLE rx;
