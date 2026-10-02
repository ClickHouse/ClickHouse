-- `reader_executor_window_size` / `reader_executor_block_size` below 4 KiB are rejected when the
-- read settings are loaded (Context::getReadSettings), independent of `use_reader_executor`.

DROP TABLE IF EXISTS t_reader_executor_min_size;
CREATE TABLE t_reader_executor_min_size (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_reader_executor_min_size SELECT number FROM numbers(1000);

SELECT sum(a) FROM t_reader_executor_min_size SETTINGS reader_executor_window_size = 100; -- { serverError INVALID_SETTING_VALUE }
SELECT sum(a) FROM t_reader_executor_min_size SETTINGS reader_executor_block_size = 4095; -- { serverError INVALID_SETTING_VALUE }

-- 4 KiB is the accepted minimum.
SELECT sum(a) FROM t_reader_executor_min_size SETTINGS reader_executor_window_size = 4096, reader_executor_block_size = 4096;

DROP TABLE t_reader_executor_min_size;
