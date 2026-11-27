SET max_block_size = 4213;
-- CI may inject adaptive aggregator freeze thresholds that merge the groups by partitions, whose blocks are not cut
-- to `max_block_size`, as the two-level buckets of a large aggregation are not either.
SET adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304, adaptive_aggregator_disable_thaw = 0;

--- We allocate space for one more row in case nullKeyData is present.
SELECT DISTINCT (blockSize() <= 4214)
FROM
(
    SELECT number
    FROM numbers(100000)
    GROUP BY number
);
