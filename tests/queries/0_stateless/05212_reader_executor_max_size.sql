-- The `ReaderExecutor` sizes share one band: `reader_executor_window_size`, `reader_executor_block_size`
-- and `reader_executor_plan_look_ahead` must each be between 128 KiB and 40 MiB. Checked when the read
-- settings are loaded (Context::getReadSettings), independent of `use_reader_executor`.

DROP TABLE IF EXISTS t_reader_executor_max_size;
CREATE TABLE t_reader_executor_max_size (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_reader_executor_max_size SELECT number FROM numbers(1000);

SELECT sum(a) FROM t_reader_executor_max_size SETTINGS reader_executor_window_size = 41943041; -- { serverError INVALID_SETTING_VALUE }
SELECT sum(a) FROM t_reader_executor_max_size SETTINGS reader_executor_block_size = 41943041; -- { serverError INVALID_SETTING_VALUE }
SELECT sum(a) FROM t_reader_executor_max_size SETTINGS reader_executor_plan_look_ahead = 41943041; -- { serverError INVALID_SETTING_VALUE }

-- 40 MiB is the accepted maximum for all three, so the shared ceiling always leaves
-- `reader_executor_plan_look_ahead >= reader_executor_block_size` satisfiable.
SELECT sum(a) FROM t_reader_executor_max_size
SETTINGS reader_executor_window_size = 41943040, reader_executor_block_size = 41943040, reader_executor_plan_look_ahead = 41943040;

DROP TABLE t_reader_executor_max_size;
