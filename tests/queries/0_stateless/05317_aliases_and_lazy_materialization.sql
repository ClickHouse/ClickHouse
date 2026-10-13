-- Tags: no-random-settings

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (id UInt64, ts DateTime, s String, s_alias String ALIAS s)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_str SELECT number, toDateTime('2026-01-01 00:00:00') + number, toString(number) FROM numbers(100);

-- Selecting both the source column and its alias must materialize both outputs.
SELECT s, s_alias FROM t_str ORDER BY ts DESC LIMIT 10;

DROP TABLE t_str;
