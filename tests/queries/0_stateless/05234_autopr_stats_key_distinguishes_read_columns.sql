-- The runtime dataflow statistics entry that feeds the automatic parallel replicas cost model is
-- keyed per plan node, and for a read that key has to describe which columns the read produces.
-- Two queries over the same table, with the same filter and the same shape above the read, still
-- move very different amounts of data when they project different columns, so they must not share
-- an entry.
--
-- Nothing else catches this. The drift check that guards a reused entry compares
-- `total_rows_to_read`, which is identical for both, and the steps between the read and the node
-- the replicas would send from are transparent for the key when they hand their inputs onward
-- unchanged - which a `Sorting` without a limit and a projection that only forwards columns both
-- do. So for the shape below the key degenerates to the read, and the read is all that can tell
-- these two queries apart.

DROP TABLE IF EXISTS t_autopr_stats_key;

CREATE TABLE t_autopr_stats_key (k UInt64, narrow UInt8, wide String) ENGINE = MergeTree ORDER BY k;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3,
    cluster_for_parallel_replicas='parallel_replicas';

SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- Otherwise the cheap pre-check rejects this table before the probe plan is built and nothing
-- collects statistics at all.
SET automatic_parallel_replicas_min_bytes_per_replica=0;

INSERT INTO t_autopr_stats_key SELECT number, number % 7, repeat('x', 200) FROM numbers(200000);

-- `automatic_parallel_replicas_mode` = 1 installs the statistics collector exactly when the lookup
-- found nothing (or found something stale), so `RuntimeDataflowStatisticsInputBytes` > 0 says "this
-- query did not reuse an entry". Mode 2 would collect unconditionally and tell us nothing.

-- Empty cache: collects.
SELECT k, narrow FROM t_autopr_stats_key ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_0_narrow_first';

-- The same query again: reuses what query 0 left behind, so it collects nothing. This is the half
-- that must keep working - the point is not to key every query separately.
SELECT k, narrow FROM t_autopr_stats_key ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_1_narrow_again';

-- Same table, same ranges, same shape, one different column - and `wide` is a two-hundred-byte
-- string where `narrow` is one byte. Reusing query 0's entry here would price this query at a
-- fraction of what it really reads, so it has to collect its own.
SELECT k, wide FROM t_autopr_stats_key ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_2_wide';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment AS query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS collected_own_statistics
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
  AND (current_database = currentDatabase()) AND (log_comment LIKE '05234_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_stats_key;
