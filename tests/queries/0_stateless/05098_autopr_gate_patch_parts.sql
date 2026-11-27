-- The gate that skips building the parallel-replicas plan sizes a read from index analysis, which
-- knows nothing about patch parts: `MergeTreeReadPoolBase` picks them per part, `addPatchPartsColumns`
-- then adds the patch key columns to the main read, and the patch parts themselves are read in full
-- without appearing among the ranges index analysis selected. On a table an update has patched, the
-- read can pull nearly twice the bytes the estimate accounts for, so the gate must not size such a
-- read at all - it declines to answer and the query stays a candidate however little it seems to read.
--
-- Patch parts are counted separately from data mutations, so a lightweight update leaves
-- `hasDataMutations` false and does not reach the guard that already covers on-fly mutation steps.
--
-- `RuntimeDataflowStatisticsInputBytes` is non-zero only for a query the gate let through and the
-- optimization then instrumented, so it is what says the query is still a candidate.

DROP TABLE IF EXISTS t_autopr_patch;

-- Far below `automatic_parallel_replicas_min_bytes_per_replica`, so the gate would reject this read
-- if it sized it at all.
CREATE TABLE t_autopr_patch (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_autopr_patch SELECT number, number FROM numbers(1000);

SET allow_experimental_lightweight_update = 1;
UPDATE t_autopr_patch SET val = val + 1 WHERE key < 500;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 1048576,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
-- The gate declines to size any read while the range-split fault injection is armed, and it checks
-- that before the patch-parts guard under test - so a randomized non-zero value would make this pass
-- even with that guard removed. `clickhouse-test` randomizes it, so pin it off.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- Reads `val`, the column the patch holds, so the patch part is actually read.
SELECT key, val FROM t_autopr_patch FORMAT Null SETTINGS log_comment = '05098_autopr_gate_patch_parts';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS still_a_candidate
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05098_autopr_gate_patch_parts')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_patch;
