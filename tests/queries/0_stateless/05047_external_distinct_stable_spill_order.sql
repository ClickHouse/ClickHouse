-- Equal zero values must collapse to one row after intermediate merges. Without an input-order
-- requirement, either binary representation is valid. Small blocks and a fan-in of two force several
-- merge levels, independently of the test runner's randomized block size. Assertions aggregate over
-- the result so predicates cannot be pushed below `DISTINCT` and eliminate the filler keys.
SELECT count(), countIf(k = 0)
FROM
(
    SELECT DISTINCT k
    FROM
    (
        SELECT if(number < 2048, (number + 10000)::Float64, if(number % 2 = 0, -0., 0.)) AS k
        FROM numbers(4096)
    )
)
SETTINGS max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 0,
    max_untracked_memory = 0, max_threads = 1, max_block_size = 127, max_external_merge_fan_in = 2,
    allow_preliminary_distinct_abandoning = 0, optimize_distinct_in_order = 0,
    log_queries = 1, log_queries_probability = 1, log_profile_events = 1,
    log_comment = '05047_spill_order/unordered';

-- Sorting by the input position requires `DISTINCT` to preserve arrival order and the first binary
-- representative of each key. The first zero is negative even when smaller files are merged first.
SELECT count(), countIf(k = 0), sumIf(reinterpretAsUInt64(k), k = 0)
FROM
(
    SELECT DISTINCT if(number < 2048, (number + 10000)::Float64, if(number % 2 = 0, -0., 0.)) AS k
    FROM numbers(4096)
    ORDER BY number + 1
)
SETTINGS max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 0,
    max_untracked_memory = 0, max_threads = 1, max_block_size = 127, max_external_merge_fan_in = 2,
    allow_preliminary_distinct_abandoning = 0, optimize_distinct_in_order = 0,
    query_plan_remove_redundant_sorting = 0,
    log_queries = 1, log_queries_probability = 1, log_profile_events = 1,
    log_comment = '05047_spill_order/ordered';

-- Both cases must write enough `DISTINCT` files to require an intermediate merge at fan-in two.
SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(ProfileEvents['ExternalDistinctWritePart'] > 2
    AND ProfileEvents['ExternalProcessingIntermediateMerge'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('05047_spill_order/unordered', '05047_spill_order/ordered');
