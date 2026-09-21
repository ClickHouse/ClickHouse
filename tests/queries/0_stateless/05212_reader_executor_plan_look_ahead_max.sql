-- `reader_executor_plan_look_ahead` above 40 MiB is rejected when the read settings are loaded
-- (Context::getReadSettings), independent of `use_reader_executor`.

DROP TABLE IF EXISTS t_reader_executor_plan_look_ahead_max;
CREATE TABLE t_reader_executor_plan_look_ahead_max (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_reader_executor_plan_look_ahead_max SELECT number FROM numbers(1000);

SELECT sum(a) FROM t_reader_executor_plan_look_ahead_max SETTINGS reader_executor_plan_look_ahead = 41943041; -- { serverError INVALID_SETTING_VALUE }

-- 40 MiB is the accepted maximum.
SELECT sum(a) FROM t_reader_executor_plan_look_ahead_max SETTINGS reader_executor_plan_look_ahead = 41943040;

DROP TABLE t_reader_executor_plan_look_ahead_max;
