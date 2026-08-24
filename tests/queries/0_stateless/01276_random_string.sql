-- Memory regression guard: the earlier `arrayMap` over `substring` form replicated a captured
-- `randomString` column and peaked above 1 GB, so a revert now fails instead of silently costing
-- that memory again. Do not drop `max_block_size`: both peaks scale with it, so without the pin
-- a cap loose enough for a high draw stops catching a revert on a low one.
-- `count() = 256` is load-bearing: `GROUP BY byte` only creates a row for a byte value that occurs,
-- so a frequency floor on its own also holds for a generator emitting one value 10000000 times.
SELECT count() = 256 AND min(c) > 30000 FROM (SELECT arrayJoin(reinterpret(randomString(100), 'Array(UInt8)')) AS byte, count() AS c FROM numbers(100000) GROUP BY byte) SETTINGS max_memory_usage = '600Mi', max_block_size = 65409;
