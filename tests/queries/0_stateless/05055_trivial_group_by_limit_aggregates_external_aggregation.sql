-- The trivial `GROUP BY ... LIMIT` optimization freezes a shared set of kept keys. That bounds
-- the number of groups, but not the size of their aggregate states: `uniqExact`, `groupArray`,
-- `topK` and the exact quantiles keep growing for the kept keys. External aggregation therefore
-- stays available after the freeze — a table that has already been rebuilt to the kept keys is
-- flushed to a temporary file and re-seeded with those keys right afterwards, so the values stay
-- exact and the query completes instead of being forced in memory until it runs out of it
-- (see `Aggregator::Params::SharedKeptKeysControl`).
--
-- `enable_analyzer = 1` is pinned because the aggregate cutoff is armed by the planner of the
-- analyzer; `enable_parallel_replicas = 0` because with parallel replicas the cutoff stays off.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_trivial_group_by_limit_query = 1;

-- Convert to a two-level table and spill as soon as the aggregation starts, so that every block
-- consumed after the freeze goes through a flush and a re-seed of the kept keys.
SET group_by_two_level_threshold = 1;
SET max_bytes_before_external_group_by = 1;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_threads = 4;
SET max_block_size = 8192;

-- Every key has exactly 100 rows with distinct values, so the aggregate values of the kept keys
-- are the same whichever five keys are kept: any row lost to a flush shows up immediately.
-- `toUInt64` is needed because `number % 1000` is a `UInt16`, for which the aggregation picks a
-- fixed hash map and the cutoff deliberately stays inert
-- (see `Aggregator::shared_kept_keys_cutoff_inert`).
SELECT min(u), max(u), sum(u), count()
FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers_mt(100000) GROUP BY k LIMIT 5);

SELECT min(l), max(l), count()
FROM (SELECT toUInt64(number % 1000) AS k, length(groupArray(number)) AS l FROM numbers_mt(100000) GROUP BY k LIMIT 5);

-- The kept keys were frozen, the tables were flushed to temporary files and re-seeded afterwards.
SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers_mt(100000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05055_kept_keys_spill';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AggregationSharedKeptKeysRebuilds'] > 0 AS kept_keys_frozen,
    ProfileEvents['ExternalAggregationWritePart'] > 0 AS spilled,
    ProfileEvents['AggregationSharedKeptKeysSpillReseeds'] > 0 AS reseeded
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05055_kept_keys_spill'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
