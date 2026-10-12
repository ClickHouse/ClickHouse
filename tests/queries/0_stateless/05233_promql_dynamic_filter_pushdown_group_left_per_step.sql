-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
DROP TABLE IF EXISTS t_promql_dfp_steps;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE t_promql_dfp_steps ENGINE = TimeSeries;

-- The steps are 1000 and 1400, so with the 5m lookback a sample is seen only at its own step.
INSERT INTO t_promql_dfp_steps (metric_name, tags, samples) VALUES
    ('requests', map('dc', 'a', 'host', 'h1'), [(toDateTime64(1000, 3), 10), (toDateTime64(1400, 3), 40)]),
    ('target_info', map('dc', 'a', 'env', 'prod'), [(toDateTime64(1000, 3), 1), (toDateTime64(1400, 3), 1)]),
    ('target_info', map('dc', 'a', 'env', 'prod_dup'), [(toDateTime64(1000, 3), 1)]),
    ('late', map('dc', 'a', 'host', 'h1'), [(toDateTime64(1400, 3), 5)]),
    ('info2', map('dc', 'a', 'env', 'prod'), [(toDateTime64(1000, 3), 1), (toDateTime64(1400, 3), 1)]),
    ('info2', map('dc', 'b', 'env', 'x'), [(toDateTime64(1000, 3), 1)]),
    ('info2', map('dc', 'b', 'env', 'y'), [(toDateTime64(1000, 3), 1)]),
    ('info3', map('dc', 'a', 'env', 'prod'), [(toDateTime64(1000, 3), 1), (toDateTime64(1400, 3), 1)]),
    ('info3', map('dc', 'b', 'env', 'x'), [(toDateTime64(1400, 3), 1)]),
    ('info3', map('dc', 'b', 'env', 'y'), [(toDateTime64(1400, 3), 1)]);

SELECT '-- a duplicate on the right side at a step where the left side is empty is not reported, as in Prometheus';
SELECT * FROM prometheusQueryRange('t_promql_dfp_steps', '(requests > 35) * on (dc) group_left (env) target_info', 1000, 1400, 400) ORDER BY tags;
SELECT * FROM prometheusQueryRange('t_promql_dfp_steps', '(requests > 35) * on (dc) group_left target_info', 1000, 1400, 400) ORDER BY tags;
SELECT * FROM prometheusQueryRange('t_promql_dfp_steps', '(requests > 35) * on (dc) group_left (env) max by (dc, env) (target_info)', 1000, 1400, 400) ORDER BY tags;
SELECT * FROM prometheusQueryRange('t_promql_dfp_steps', 'late * on (dc) group_left (env) info2', 1000, 1400, 400) ORDER BY tags;

SELECT '-- a duplicate on the right side at a step where the left side has a sample is reported, also in a join group the left side does not have';
SELECT * FROM prometheusQueryRange('t_promql_dfp_steps', 'late * on (dc) group_left (env) info3', 1000, 1400, 400); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE t_promql_dfp_steps;
