-- Tags: no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage

-- Force an error to populate system.error_log
SELECT throwIf(true, '04041_error_log_table_symbols_lines'); -- { serverError 395 }

SYSTEM FLUSH LOGS error_log;

-- Check symbols contain Exception
SELECT arrayExists(x -> x LIKE '%Exception%', last_error_symbols)
FROM system.error_log
WHERE code = 395 AND last_error_message LIKE '%04041_error_log_table_symbols_lines%'
ORDER BY last_error_time DESC
LIMIT 1;

-- Check lines have file:line:column format
SELECT arrayExists(x -> x LIKE '%:%:%', last_error_lines)
FROM system.error_log
WHERE code = 395 AND last_error_message LIKE '%04041_error_log_table_symbols_lines%'
ORDER BY last_error_time DESC
LIMIT 1;
