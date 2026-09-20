-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
DROP TABLE IF EXISTS t_promql_dfp;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE t_promql_dfp ENGINE = TimeSeries;

-- Left-side metric `requests` with dc=a (hosts h1, h2)
-- Right-side metric `target_info` with dc=a, duplicate dc=b, and dc=c
INSERT INTO t_promql_dfp (metric_name, tags, samples) VALUES
    ('requests', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20)]),
    ('requests', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 30), (toDateTime64(110, 3), 40)]),
    ('target_info', map('dc', 'a', 'env', 'prod'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1)]),
    ('target_info', map('dc', 'b', 'env', 'staging'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1)]),
    ('target_info', map('dc', 'b', 'env', 'staging_dup'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1)]),
    ('target_info', map('dc', 'c', 'env', 'dev'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 1)]);

SELECT '-- group_left with dynamic filter pushdown pruning dc=b and dc=c';
SELECT * FROM prometheusQuery('t_promql_dfp', 'requests * on (dc) group_left (env) target_info', 110) ORDER BY tags;

SELECT '-- group_left with empty left side';
SELECT * FROM prometheusQuery('t_promql_dfp', 'requests{dc="nonexistent"} * on (dc) group_left (env) target_info', 110) ORDER BY tags;

SELECT '-- explain plan verifies join_group filter pushdown';
SELECT countIf(explain LIKE '%Filter%' AND (explain LIKE '%timeSeriesRemoveAllTagsExcept%' OR explain LIKE '%join_group%')) > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM prometheusQuery('t_promql_dfp', 'requests * on (dc) group_left (env) target_info', 110));

SELECT '-- group_left with label_replace on right side';
SELECT * FROM prometheusQuery('t_promql_dfp', 'label_replace(requests, "dc2", "$1", "dc", "(.*)") * on (dc2) group_left (env) label_replace(target_info, "dc2", "$1", "dc", "(.*)")', 110) ORDER BY tags;

SELECT '-- group_left with vector(scalar(...)) on right side';
SELECT * FROM prometheusQuery('t_promql_dfp', 'requests * on () group_left vector(scalar(sum(requests)))', 110) ORDER BY tags;

SELECT '-- group_left with on(__name__) and rate';
INSERT INTO t_promql_dfp (metric_name, tags, samples) VALUES
    ('rate_target', map('dc', 'a', 'env', 'prod'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 4)]);
SELECT * FROM prometheusQuery('t_promql_dfp', 'rate(requests{host="h1"}[50s]) * on (__name__) group_left rate(rate_target[50s])', 110) ORDER BY tags;

SELECT '-- group_left with absent on right side';
SELECT * FROM prometheusQuery('t_promql_dfp', 'requests * on (dc) group_left absent(nonexistent_metric{dc="a"})', 110) ORDER BY tags;

SELECT '-- group_left with absent returning empty';
SELECT * FROM prometheusQuery('t_promql_dfp', 'requests * on (dc) group_left absent(target_info{dc="a"})', 110) ORDER BY tags;

DROP TABLE t_promql_dfp;
