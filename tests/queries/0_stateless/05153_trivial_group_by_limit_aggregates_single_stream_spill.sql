-- On the branches where the streams do not share one kept key set — a single stream, and
-- `skip_merging`, where the streams hold disjoint keys — every stream caps itself and prunes its
-- hash table to its own kept keys. External aggregation has to stay available there too: the
-- number of kept keys is bounded, but the size of their aggregate states is not (`uniqExact`,
-- `groupArray`, `topK`, the exact quantiles). A flush leaves the kept keys in the table with
-- null states and writes only the states accumulated since the previous flush, so the values
-- stay exact and the query spills instead of being forced in memory
-- (see `Aggregator::keepsKeysAcrossSpills` and `AggregatingTransform::applyPerStreamKeptKeysCutoff`).
--
-- `enable_parallel_replicas = 0` is pinned because with parallel replicas the cutoff stays off.

SET enable_parallel_replicas = 0;
SET optimize_trivial_group_by_limit_query = 1;
-- The `GROUP BY` top-K heap takes this shape over from the kept-keys cutoff wherever it applies.
SET enable_group_by_top_k_optimization = 0;

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

-- The single stream capped itself, pruned its table to the kept keys and flushed it to a
-- temporary file, keeping the kept keys in it.
SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers(100000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS log_comment = '05153_single_stream_kept_keys_spill';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AggregationSharedKeptKeysRebuilds'] > 0 AS kept_keys_captured,
    ProfileEvents['ExternalAggregationWritePart'] > 0 AS spilled,
    ProfileEvents['AggregationKeptKeysSpills'] > 0 AS kept_keys
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05153_single_stream_kept_keys_spill'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
