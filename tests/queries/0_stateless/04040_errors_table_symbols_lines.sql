-- Tags: no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage

-- Force an error to populate system.errors
SELECT throwIf(true, '04040_errors_table_symbols_lines'); -- { serverError 395 }

-- Check symbols contain Exception
SELECT arrayExists(x -> x LIKE '%Exception%', last_error_symbols),
       arrayExists(x -> x LIKE '%:%:%', last_error_lines)
FROM system.errors
WHERE code = 395
ORDER BY last_error_time DESC
LIMIT 1
FORMAT CSV;
