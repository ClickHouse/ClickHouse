-- Query shapes the automatic parallel replicas optimization does not support yet. Each of them must
-- run, and must collect no dataflow statistics at all - `automatic_parallel_replicas_mode = 2` asks
-- only for the statistics, and an unsupported shape leaves both byte counters at zero. Once a shape
-- becomes supported its counters stop being zero and this test fails, which is the signal to drop
-- the case from here and cover the new behaviour elsewhere. The reason a shape is rejected is
-- printed by `optimizeTree` and `RuntimeDataflowStatisticsCacheUpdater` at the debug level.

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- The optimization works on the query plan, which only the analyzer builds. `InterpreterSelectQuery`
-- turns `enable_parallel_replicas` off when it sees this settings combination, so with the old
-- analyzer every shape below - supported or not - would collect no statistics.
SET enable_analyzer=1;

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

-- The preliminary `LIMIT` puts a `Limit` step on top of the read from the other replicas, and
-- `findTopNodeOfReplicasPlan` only looks through `Expression`, `Filter` and `CreatingSets` steps, so
-- it does not recognize that branch and reports "Top node for parallel replicas plan is already
-- found". This holds for any query with a `LIMIT`, not only for a trivial read.
SELECT * FROM t LIMIT 1 FORMAT Null
SETTINGS log_comment='unsupported_limit';

-- `min` over the primary key is answered by the implicit `minmax_count` projection, so the plan
-- reads no data at all: "Unsupported steps: ReadFromPreparedSource". The projection settings are
-- pinned because the test runner randomizes them, and with the projection off the very same query
-- reads the column and is supported.
SELECT min(a) FROM tt FORMAT Null
SETTINGS optimize_aggregation_in_order=0, optimize_use_projections=1, optimize_use_implicit_projections=1,
    log_comment='unsupported_min_answered_from_projection';

-- Supported shapes, so that a change which stops the optimization from running everywhere fails
-- this test instead of making every case above trivially pass. The join is here because the shape
-- used to be listed as unsupported, while what the case actually hit was the `LIMIT` above.
SELECT count() FROM t WHERE number > 5 FORMAT Null
SETTINGS log_comment='supported_aggregation';

-- `query_plan_optimize_join_order_randomize` is pinned off: the single-node plan and the plan with
-- parallel replicas are built independently, so a randomized join order makes them diverge and the
-- optimization finds no matching node to instrument.
SELECT * FROM t lhs INNER JOIN t rhs ON lhs.number = rhs.number FORMAT Null
SETTINGS query_plan_optimize_join_order_randomize=0, log_comment='supported_join';

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
