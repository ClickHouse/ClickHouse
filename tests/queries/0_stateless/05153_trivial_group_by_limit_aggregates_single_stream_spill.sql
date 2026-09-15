-- On the branches where the streams do not share one kept key set — a single stream, and
-- `skip_merging`, where the streams hold disjoint keys — every stream caps itself, so its hash
-- table holds nothing but its own kept keys. External aggregation has to stay available there
-- too: the number of kept keys is bounded, but the size of their aggregate states is not
-- (`uniqExact`, `groupArray`, `topK`, the exact quantiles). The stream captures its own kept keys
-- as a seed and the `Aggregator` re-seeds the emptied table with them right after the flush, so
-- the values stay exact and the query spills instead of being forced in memory
-- (see `AggregatingTransform::capturePerStreamKeptKeysSeed`).
--
-- `enable_analyzer = 1` is pinned because the aggregate cutoff is armed by the planner of the
-- analyzer; `enable_parallel_replicas = 0` because with parallel replicas the cutoff stays off.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_trivial_group_by_limit_query = 1;

-- A single aggregating stream, converted to a two-level table and spilling as soon as the
-- aggregation starts, so that every block consumed after the cap goes through a flush and a
-- re-seed of the kept keys.
SET max_threads = 1;
SET group_by_two_level_threshold = 1;
SET max_bytes_before_external_group_by = 1;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_block_size = 8192;

-- Every key has exactly 100 rows with distinct values, so the aggregate values of the kept keys
-- are the same whichever five keys are kept: any row lost to a flush shows up immediately.
-- `toUInt64` is needed because `number % 1000` is a `UInt16`, for which the aggregation picks a
-- fixed hash map and the cutoff deliberately stays inert
-- (see `Aggregator::shared_kept_keys_cutoff_inert`).
SELECT min(u), max(u), sum(u), count()
FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers(100000) GROUP BY k LIMIT 5);

SELECT min(l), max(l), count()
FROM (SELECT toUInt64(number % 1000) AS k, length(groupArray(number)) AS l FROM numbers(100000) GROUP BY k LIMIT 5);

-- The single stream capped itself, flushed its table to a temporary file and re-seeded it with
-- its kept keys afterwards.
SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers(100000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05153_single_stream_kept_keys_spill';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AggregationSharedKeptKeysRebuilds'] > 0 AS kept_keys_captured,
    ProfileEvents['ExternalAggregationWritePart'] > 0 AS spilled,
    ProfileEvents['AggregationSharedKeptKeysSpillReseeds'] > 0 AS reseeded
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05153_single_stream_kept_keys_spill'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
