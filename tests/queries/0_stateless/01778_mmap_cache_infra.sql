-- Tags: no-parallel
-- We check the existence of queries and metrics and don't check the results (a smoke test).

SYSTEM DROP MMAP CACHE;

SET system_events_show_zero_values = 1;
SELECT event FROM system.events WHERE event LIKE '%MMap%' ORDER BY event;
SELECT metric FROM system.metrics WHERE metric LIKE '%MMap%' ORDER BY metric;
