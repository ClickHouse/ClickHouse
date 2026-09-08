-- Verify how query plans containing a Window step interact with the automatic parallel replicas
-- optimization. Before the Window step supported dataflow statistics collection, any plan containing a
-- window function was rejected outright (`optimizeTree: Some steps in the plan don't support dataflow
-- statistics collection ... Unsupported steps: Window_...`) and no statistics were gathered. Now the
-- plan passes the "simple enough" gate, and statistics are collected at whichever boundary the two
-- plans have in common, as long as that boundary can observe the bytes replicas would send to the
-- initiator.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET automatic_parallel_replicas_min_bytes_per_replica=0;

INSERT INTO t SELECT number, number * 2 FROM numbers(1e6);

-- A window function over a bare table scan: with parallel replicas the replicas execute only the reading
-- step, since the window itself is computed on the initiator. The reading step records input bytes only,
-- so `findTopNodeOfReplicasPlan` stops one step above it rather than peeling all the way down, and the
-- wrapper it stops on does record output bytes - the pre-window rows the replicas would ship. Statistics
-- are therefore collected for this query, and the cost model gets to decide on real numbers instead of
-- being skipped: the wrapper passes its rows through, so output comes out roughly equal to input, the
-- replicas cost more than reading locally, and the optimization is not applied. Before the boundary was
-- moved above the read this shape failed close, matching the reading step and collecting nothing.
SELECT key, sum(value) OVER (PARTITION BY key % 10 ORDER BY key) AS s
FROM t
FORMAT Null SETTINGS log_comment='04502_autopr_window_function_query';

-- Regression guard for the output side. The window is computed on the coordinator, so the columns it
-- appends are never sent to the initiator and must never be recorded as replica output. Here the real
-- replica-output boundary is the Aggregating step (it ships partial aggregation states to the
-- initiator), and the Window step sits above it building a very wide result (`groupArray` over a 50-row
-- frame on ~100k groups, so the window output is far larger than everything read from the table). The
-- recorded output bytes must stay bounded by the input bytes: if the window result were mistakenly
-- counted as replica output, the recorded output bytes would balloon past the input bytes and this
-- check would fail.
SELECT key % 100000 AS k, sum(value) AS s, groupArray(sum(value)) OVER (ORDER BY k ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) AS a
FROM t
GROUP BY k
FORMAT Null SETTINGS log_comment='04502_autopr_window_wide_output';

-- Regression for the aggregated-window shape: a window function computed on the initiator ON TOP of an
-- aggregation that runs on the replicas. Here the real replica-output boundary is the Aggregating step
-- (it ships partial aggregation states to the initiator), and the Window step sits ABOVE it. Because
-- `calculateHashTableCacheKeys` folds every node's children into a fresh hash round (a row-preserving
-- step such as Window contributes no bytes of its own but is still hashed as a distinct wrapper of its
-- child), the Window step's cache key strictly differs from the aggregation's, so
-- `findCorrespondingNodeInSingleNodePlan` cannot mis-select the (uninstrumented) Window step as the
-- boundary. If it did, the recorded output bytes would collapse to zero and the cost model would forget
-- that replicas still ship the aggregation result. This query keeps aggregation and window in a single
-- SELECT: `sum(sum(value)) OVER (...)` is a window function evaluated over the aggregate `sum(value)`.
SELECT key % 10 AS k, sum(value) AS s, sum(sum(value)) OVER (ORDER BY key % 10) AS running
FROM t
GROUP BY k
FORMAT Null SETTINGS log_comment='04502_autopr_aggregated_window';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The bare-scan window query is instrumented at the wrapper above the read, which sees the pre-window
-- rows. Statistics must be collected, and the recorded output must be a real measurement rather than the
-- `0` a read boundary would report - caching `output_bytes = 0` would make the cost model treat the
-- network transfer of all pre-window rows as free. The wrapper ships what it read, so its output stays
-- in the same range as its input.
SELECT log_comment,
    (ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0)
        AND (ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0)
        AND (ProfileEvents['RuntimeDataflowStatisticsOutputBytes']
             <= (2 * ProfileEvents['RuntimeDataflowStatisticsInputBytes']))
        AS output_measured_above_read
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '04502_autopr_window_function_query') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

SELECT
    maxIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = '04502_autopr_window_wide_output') > 0 AS stats_collected,
    (maxIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '04502_autopr_window_wide_output') > 0)
        AND (maxIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '04502_autopr_window_wide_output')
             <= maxIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = '04502_autopr_window_wide_output'))
        AS window_result_not_counted_as_output
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '04502_autopr_window_wide_output') AND (type = 'QueryFinish')
FORMAT TSVWithNames;

-- For the aggregated-window query the replica-output boundary is the Aggregating step, which DOES collect
-- output bytes (the aggregation states shipped to the initiator). Statistics must be collected (the plan
-- passed the "simple enough" gate) and the recorded output must be pinned to that aggregation boundary:
-- non-zero (it did not collapse to the uninstrumented Window step) and bounded by the input bytes (the
-- window result was not counted as replica output).
SELECT
    maxIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = '04502_autopr_aggregated_window') > 0 AS stats_collected,
    (maxIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '04502_autopr_aggregated_window') > 0)
        AND (maxIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '04502_autopr_aggregated_window')
             <= maxIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = '04502_autopr_aggregated_window'))
        AS output_pinned_to_aggregation_boundary
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '04502_autopr_aggregated_window') AND (type = 'QueryFinish')
FORMAT TSVWithNames;

DROP TABLE t;
