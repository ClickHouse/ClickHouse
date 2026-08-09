-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A `Merge` table materializes its child plans lazily, while the outer plan is already executing.
-- `make_distributed_plan` must not leak into the child pipelines: `ReadFromMerge::buildPipeline`
-- used to pass it through to `QueryPlan::buildQueryPipeline`, which converted the child plan to a
-- distributed one mid-execution. That failed with `Unknown query plan step: GatherReceive`, and with
-- `Cannot serialize FutureSetFromSubquery with no query plan` when the query had subquery sets
-- (already consumed by the outer plan's `addStepsToBuildSets`).
-- Related: https://github.com/ClickHouse/ClickHouse/pull/112849

DROP TABLE IF EXISTS t_merge_dp;
DROP TABLE IF EXISTS m_merge_dp;
CREATE TABLE t_merge_dp (x UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Large enough that the distributed conversion does not keep the read local by the size heuristic.
INSERT INTO t_merge_dp SELECT number FROM numbers(100000);
CREATE TABLE m_merge_dp (x UInt64) ENGINE = Merge(currentDatabase(), '^t_merge_dp$');

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0 (randomized
-- settings set it nonzero, which would make make_distributed_plan reject queries at planning time).
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
-- A single stream, so the row order of the child read is deterministic.
SET max_threads = 1;

SELECT x FROM m_merge_dp WHERE x < 5;
SELECT x FROM m_merge_dp WHERE x GLOBAL IN (SELECT number FROM numbers(3));

DROP TABLE m_merge_dp;
DROP TABLE t_merge_dp;
