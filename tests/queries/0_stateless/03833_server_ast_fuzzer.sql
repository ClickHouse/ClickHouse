-- Tags: no-fasttest

-- Suppress error-level log messages from fuzzed queries that fail expectedly.
SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 3;

SELECT 1;

SELECT number FROM numbers(10) WHERE number > 5 ORDER BY number;
