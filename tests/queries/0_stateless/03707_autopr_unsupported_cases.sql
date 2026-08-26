-- Query shapes the automatic parallel replicas optimization does not support yet. Each of them must
-- run, and must collect no dataflow statistics at all - `automatic_parallel_replicas_mode = 2` asks
-- only for the statistics, and an unsupported shape leaves both byte counters at zero. Once a shape
-- becomes supported its counters stop being zero and this test fails, which is the signal to drop
-- the case from here and cover the new behaviour elsewhere. The reason a shape is rejected is
-- printed by `optimizeTree` and `RuntimeDataflowStatisticsCacheUpdater` at the debug level.

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

CREATE TABLE t(number UInt64) ENGINE=MergeTree ORDER BY () AS SELECT * FROM numbers_mt(1e6);

CREATE TABLE tt
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a
AS SELECT
    number,
    number * 2
FROM numbers_mt(1e5);

-- The plan of the outer aggregation has no counterpart in the single-node plan:
-- "Cannot find step with matching hash in single-node plan".
SELECT AVG(transfer) FROM (SELECT number, SUM(number) AS transfer FROM t GROUP BY number) FORMAT Null
SETTINGS log_comment='unsupported_aggregation_over_aggregation';

-- "Unsupported steps: Union".
SELECT * FROM t UNION ALL SELECT * FROM t FORMAT Null
SETTINGS log_comment='unsupported_union_all';

-- Two tables to read from, while the optimization instruments a single source:
-- "Top node for parallel replicas plan is already found".
SELECT * FROM t lhs INNER JOIN t rhs ON lhs.number = rhs.number LIMIT 1 FORMAT Null
SETTINGS log_comment='unsupported_join';

-- `min` over the primary key is answered from the index, so the plan reads no data at all:
-- "Unsupported steps: ReadFromPreparedSource".
SELECT min(a) FROM tt FORMAT Null
SETTINGS optimize_aggregation_in_order=0, log_comment='unsupported_min_answered_from_index';

-- A supported shape, so that a change which stops the optimization from running everywhere fails
-- this test instead of making every case above trivially pass.
SELECT count() FROM t WHERE number > 5 FORMAT Null
SETTINGS log_comment='supported_aggregation';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

WITH collected AS
(
    SELECT
        log_comment,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes,
        ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= now() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (type = 'QueryFinish')
)
SELECT * FROM
(
    SELECT log_comment, input_bytes, output_bytes FROM collected WHERE log_comment LIKE 'unsupported_%'
    UNION ALL
    SELECT log_comment, input_bytes > 0, output_bytes > 0 FROM collected WHERE log_comment LIKE 'supported_%'
)
ORDER BY log_comment;
