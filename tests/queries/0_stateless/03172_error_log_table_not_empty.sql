-- Throw three random errors: 111, 222 and 333
SELECT throwIf(true, 'error_log', toInt16(111)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 111 }
SELECT throwIf(true, 'error_log', toInt16(222)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 222 }
SELECT throwIf(true, 'error_log', toInt16(333)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 333 }

-- Wait for more than collect_interval_milliseconds to ensure system.error_log is flushed
SELECT sleep(2) FORMAT NULL;
SYSTEM FLUSH LOGS;

-- Check that the three random errors are propagated
SELECT sum(value) > 0 FROM system.error_log WHERE code = 111 AND event_time > now() - INTERVAL 1 MINUTE;
SELECT sum(value) > 0 FROM system.error_log WHERE code = 222 AND event_time > now() - INTERVAL 1 MINUTE;
SELECT sum(value) > 0 FROM system.error_log WHERE code = 333 AND event_time > now() - INTERVAL 1 MINUTE;

-- Ensure that if we throw them again, they're still propagated
SELECT throwIf(true, 'error_log', toInt16(111)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 111 }
SELECT throwIf(true, 'error_log', toInt16(222)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 222 }
SELECT throwIf(true, 'error_log', toInt16(333)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 333 }

SELECT sleep(2) FORMAT NULL;
SYSTEM FLUSH LOGS;

SELECT sum(value) > 1 FROM system.error_log WHERE code = 111 AND event_time > now() - INTERVAL 1 MINUTE;
SELECT sum(value) > 1 FROM system.error_log WHERE code = 222 AND event_time > now() - INTERVAL 1 MINUTE;
SELECT sum(value) > 1 FROM system.error_log WHERE code = 333 AND event_time > now() - INTERVAL 1 MINUTE;