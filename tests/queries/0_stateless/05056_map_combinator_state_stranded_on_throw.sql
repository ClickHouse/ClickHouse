-- `-Map` add and mergeImpl used to `create` a nested state and only then `emplace` it into
-- `merged_maps`. `emplace` allocates a node, and a copy of the key when it is a `String`, so it can
-- throw on the memory limit after the state is already built. The state was then unreachable and
-- undestroyed, because `destroyImpl` walks only `merged_maps`. `quantileDD` shows it: its `create`
-- builds a `DDSketch` that owns three heap allocations, and a throw part-way through the loop
-- stranded them. Under a leak sanitizer this query reported 160 bytes in 3 allocations before the
-- fix. The limit is what arms the window, so the query is expected to hit it.
SELECT length(quantileDDMap(0.001, 0.5)(m))
FROM (SELECT map(repeat(concat('k', toString(number)), 12), toFloat64(number)) AS m FROM numbers(20000))
SETTINGS max_threads = 1, max_untracked_memory = 1, max_block_size = 1024, max_memory_usage = 8000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- The same aggregation without the limit is unaffected.
SELECT length(quantileDDMap(0.001, 0.5)(m))
FROM (SELECT map(repeat(concat('k', toString(number)), 12), toFloat64(number)) AS m FROM numbers(20000));
