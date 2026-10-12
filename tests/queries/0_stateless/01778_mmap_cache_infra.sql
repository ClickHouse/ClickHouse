-- Tags: no-parallel:misc-caches
-- Tag no-parallel: serializes tests that mutate or assert the shared `misc-caches` resource
-- (this test issues `SYSTEM CLEAR MMAP CACHE`, which is process-wide)
-- We check the existence of queries and metrics and don't check the results (a smoke test).

SYSTEM CLEAR MMAP CACHE;

SET system_events_show_zero_values = 1;
SELECT event FROM system.events WHERE event LIKE '%MMap%' ORDER BY event;
SELECT metric FROM system.metrics WHERE metric LIKE '%MMap%' ORDER BY metric;
