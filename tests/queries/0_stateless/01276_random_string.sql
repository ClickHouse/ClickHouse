-- Memory regression guard: the earlier `arrayMap` over `substring` form replicated a captured
-- `randomString` column and peaked above 1 GB, so a revert now fails instead of silently costing
-- that memory again. Do not drop `max_block_size`: both peaks scale with it, so without the pin
-- a cap loose enough for a high draw stops catching a revert on a low one. The zero fault
-- probability keeps the stress runner's injected `0.001` from making a tight cap flaky.
SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(reinterpret(randomString(100), 'Array(UInt8)')) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte) SETTINGS max_memory_usage = '600Mi', memory_tracker_fault_probability = 0, max_block_size = 65409;
