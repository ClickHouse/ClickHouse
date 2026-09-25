SET max_threads = 4;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 128;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_ratio_before_external_group_by = 0;

-- Aggregate states produced by one aggregation become arguments to the next aggregation.
-- Each key has five source rows split between two intermediate groups. The exact per-key
-- checks cover heap-owned states, nullable arguments, and state lifetimes across both stages.
SET max_bytes_before_external_group_by = 0;
SELECT 'resident states', count() = 60000,
    countIf(s != 5 * k + 600000 OR u != 5 OR isNull(m) OR m != k + 60000) = 0
FROM
(
    SELECT k, sumMerge(s_state) AS s, uniqExactMerge(u_state) AS u, minMerge(m_state) AS m
    FROM
    (
        SELECT toUInt64(number % 60000) AS k, intDiv(number, 60000) % 2 AS part,
            sumState(number) AS s_state,
            uniqExactState(number) AS u_state,
            minState(if(number < 60000, NULL, toNullable(number))) AS m_state
        FROM numbers_mt(300000)
        GROUP BY k, part
    )
    GROUP BY k
);

SET max_bytes_before_external_group_by = 1000000;
SELECT 'external states', count() = 60000,
    countIf(s != 5 * k + 600000 OR u != 5 OR isNull(m) OR m != k + 60000) = 0
FROM
(
    SELECT k, sumMerge(s_state) AS s, uniqExactMerge(u_state) AS u, minMerge(m_state) AS m
    FROM
    (
        SELECT toUInt64(number % 60000) AS k, intDiv(number, 60000) % 2 AS part,
            sumState(number) AS s_state,
            uniqExactState(number) AS u_state,
            minState(if(number < 60000, NULL, toNullable(number))) AS m_state
        FROM numbers_mt(300000)
        GROUP BY k, part
    )
    GROUP BY k
);
