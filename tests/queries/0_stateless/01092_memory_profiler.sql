-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-parallel, no-fasttest, no-cpu-aarch64

SET allow_introspection_functions = 1;

SET memory_profiler_step = 1000000;
SET memory_profiler_sample_probability = 1;
SET log_queries = 1;

SELECT ignore(groupArray(number), 'test memory profiler') FROM numbers(10000000);
SYSTEM FLUSH LOGS;
WITH addressToSymbol(arrayJoin(trace)) AS symbol SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND trace_type = 'Memory' AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%test memory profiler%' ORDER BY event_time DESC LIMIT 1);
WITH addressToSymbol(arrayJoin(trace)) AS symbol SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND trace_type = 'MemoryPeak' AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%test memory profiler%' ORDER BY event_time DESC LIMIT 1);
WITH addressToSymbol(arrayJoin(trace)) AS symbol SELECT count() > 0 FROM system.trace_log t WHERE event_date >= yesterday() AND trace_type = 'MemorySample' AND query_id = (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%test memory profiler%' ORDER BY event_time DESC LIMIT 1);
