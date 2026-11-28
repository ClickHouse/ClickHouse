SELECT randomFixedString('string'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT randomFixedString(0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomFixedString(rand() % 10); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(randomFixedString(10));
-- Memory regression guard: the earlier `arrayMap` over `substring` form replicated a captured
-- `randomFixedString` column and peaked above 1 GB, so a revert now fails instead of silently costing
-- that memory again. Do not drop `max_block_size`: both peaks scale with it, so without the pin
-- a cap loose enough for a high draw stops catching a revert on a low one.
SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(reinterpret(randomFixedString(100), 'Array(UInt8)')) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte) SETTINGS max_memory_usage = '600Mi', max_block_size = 65409;
