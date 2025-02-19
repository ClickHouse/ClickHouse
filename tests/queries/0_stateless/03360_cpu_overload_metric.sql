-- A crude test. Overloads the server for two seconds and checks that the overload was recorded.
SELECT count() FROM system.numbers_mt SETTINGS max_threads = 256, use_concurrency_control = 0, max_execution_time = 2, timeout_overflow_mode = 'break';
SYSTEM FLUSH LOGS;
SELECT 1 FROM system.asynchronous_metric_log WHERE event_date >= yesterday() AND metric = 'OSCPUOverload' AND value >= 1 ORDER BY event_time DESC LIMIT 1;
