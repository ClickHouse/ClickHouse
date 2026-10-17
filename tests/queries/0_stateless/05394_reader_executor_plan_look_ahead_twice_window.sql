-- `reader_executor_plan_look_ahead` must be at least twice `reader_executor_window_size`: the plan refills once
-- half of it is left, so it then always covers a whole fetch. Checked when the read settings are loaded
-- (Context::getReadSettings), independent of `use_reader_executor`.

DROP TABLE IF EXISTS t_reader_executor_plan_twice_window;
CREATE TABLE t_reader_executor_plan_twice_window (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_reader_executor_plan_twice_window SELECT number FROM numbers(1000);

SELECT sum(a) FROM t_reader_executor_plan_twice_window
SETTINGS reader_executor_window_size = 8388608, reader_executor_plan_look_ahead = 16777215; -- { serverError INVALID_SETTING_VALUE }
SELECT sum(a) FROM t_reader_executor_plan_twice_window
SETTINGS reader_executor_window_size = 8388608, reader_executor_plan_look_ahead = 16777216;

DROP TABLE t_reader_executor_plan_twice_window;
