SET allow_introspection_functions = 1;

SET query_profiler_real_time_period_ns = 100000000;
SET log_queries = 1;
SELECT sleep(0.5), ignore('test real time query profiler');
SET log_queries = 0;
SYSTEM FLUSH LOGS;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE query_id = (SELECT query_id FROM system.query_log WHERE query LIKE '%test real time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%FunctionSleep%';

SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1;
SELECT count(), ignore('test cpu time query profiler') FROM numbers(1000000000);
SET log_queries = 0;
SYSTEM FLUSH LOGS;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE query_id = (SELECT query_id FROM system.query_log WHERE query LIKE '%test cpu time query profiler%' AND query NOT LIKE '%system%' ORDER BY event_time DESC LIMIT 1) AND symbol LIKE '%Source%';
