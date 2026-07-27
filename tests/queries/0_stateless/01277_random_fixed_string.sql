SELECT randomFixedString('string'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT randomFixedString(0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomFixedString(rand() % 10); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(randomFixedString(10));
-- The `SETTINGS` clause is a regression guard, not a workaround: the earlier `arrayMap` over `substring` form replicated a captured
-- `randomFixedString` column and peaked well above 1 GB, while sampling the bytes via `reinterpret` to `Array(UInt8)` stays under
-- 100 MB, so a revert fails on `max_memory_usage` instead of silently costing that memory again. `max_block_size` is pinned because
-- both peaks scale with it: with the runner's randomized value no constant cap is both safe at a high draw and discriminating at a
-- low one, so dropping the pin would leave the guard inert on low draws. `memory_tracker_fault_probability = 0` neutralises the
-- stress runner's injected `0.001`, which would otherwise make a tight cap flaky.
SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(reinterpret(randomFixedString(100), 'Array(UInt8)')) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte) SETTINGS max_memory_usage = '600Mi', memory_tracker_fault_probability = 0, max_block_size = 65409;
