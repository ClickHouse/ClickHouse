SET query_plan_optimize_lazy_materialization=1;
SET query_plan_max_limit_for_lazy_materialization=10;
CREATE TABLE test (x Int, d Dynamic) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO test SELECT number, number FROM numbers(3);
SELECT d.Date, d, d.String FROM test ORDER BY materialize(1), x DESC SETTINGS limit = 1;

