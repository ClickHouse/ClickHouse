
DROP TABLE IF EXISTS file_log;
CREATE TABLE file_log  (`key` UInt8, `value` UInt8) ENGINE = FileLog('./user_files/02889_file_log_save_errors_test_11/', 'JSONEachRow') SETTINGS handle_error_mode = 'stream'
;

SET stream_like_engine_allow_direct_select = 1;
SET send_logs_level = 'error'; -- Reading from empty file log produces warning
select * from merge('', 'file_log');
