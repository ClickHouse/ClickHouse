-- `quantileDeterministic` must give the same answer when the aggregation spills its partial states
-- to temporary files. The aggregator hands the spill writer unversioned `AggregateFunction` types,
-- so the reservoir's skip degree survives the round trip only because the temporary streams are
-- written at the current protocol revision, which versions those states the same way as the wire.
-- https://github.com/ClickHouse/ClickHouse/pull/112052

-- One group fed by two very unequal blocks: each flush turns the states accumulated so far into a
-- temporary file of its own, so the merge sees the same lopsided 990000/10000 split that used to
-- give a different answer when serialization dropped the skip degree (506014 instead of 492708).
-- Spilling needs a two-level hash table, which a single group never reaches on its own, hence the
-- explicit `group_by_two_level_threshold`.
SELECT medianDeterministic(number, number)
FROM numbers(1000000)
GROUP BY intDiv(number, 1000000)
SETTINGS max_threads = 1,
         max_block_size = 990000,
         max_bytes_before_external_group_by = 1,
         max_bytes_ratio_before_external_group_by = 0,
         group_by_two_level_threshold = 1,
         log_comment = '04824_quantile_deterministic_external_aggregation_spill';

-- The aggregation above must have actually spilled; otherwise the test proves nothing.
SYSTEM FLUSH LOGS query_log;
SELECT max(ProfileEvents['ExternalAggregationWritePart']) >= 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04824_quantile_deterministic_external_aggregation_spill'
    AND type = 'QueryFinish';
