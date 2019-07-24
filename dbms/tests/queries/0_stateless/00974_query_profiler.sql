SET query_profiler_real_time_period_ns = 100000000;
SELECT sleep(0.5), ignore('test real time query profiler');
SYSTEM FLUSH LOGS;
WITH symbolizeAddress(arrayJoin(trace)) AS symbol SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND query_id = (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND query LIKE '%test real time query profiler%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%FunctionSleep%';

SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 100000000;
SELECT count(), ignore('test cpu time query profiler') FROM numbers(1000000000);
SYSTEM FLUSH LOGS;
WITH symbolizeAddress(arrayJoin(trace)) AS symbol SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND query_id = (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND query LIKE '%test cpu time query profiler%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Numbers%';
