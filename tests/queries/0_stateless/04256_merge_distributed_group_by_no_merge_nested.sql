-- Tags: distributed

-- Follow-up to #100859 / #75604.
--
-- The original fix relaxed the `to_stage == Complete` assertion in
-- `StorageDistributed::getQueryProcessingStage` so that a multi-table
-- `StorageMerge` wrapping a `Distributed` table no longer crashes with
-- `LOGICAL_ERROR` under `distributed_group_by_no_merge=1`. It only covered
-- `count` / `sum`, where the partial state and the final value share the
-- same column type.
--
-- For aggregate functions whose state differs from the final type
-- (`uniqExact`, `avg`, `groupArray`, ...) the path through
-- `ReadFromMerge::createPlanForTable`'s `must_return_interpreter_select_query_plan`
-- branch (triggered when a `Merge` wraps another `Merge`) used to surface as
-- `CANNOT_CONVERT_TYPE` because the inner `StorageMerge` propagated `Complete`
-- upward (single-table case, no cap) and the outer `Merge`'s `common_header`
-- was built at `WithMergeableState` (expecting `AggregateFunction` states).

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS m_inner;

CREATE TABLE t_mt (c0 Int) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_mt SELECT * FROM numbers(10);

CREATE TABLE t_dist (c0 Int) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_mt');

CREATE TABLE t_mem (c0 Int) ENGINE = Memory;
INSERT INTO t_mem SELECT * FROM numbers(10);

-- Inner Merge wraps a single Distributed - forces the single-table branch
-- of `StorageMerge::getQueryProcessingStage` (no implicit cap to WithMergeableState).
CREATE TABLE m_inner (c0 Int) ENGINE = Merge(currentDatabase(), '^t_dist$');

-- { echoOn }
-- 1. Same column type at WithMergeableState and Complete (count / sum) - worked even before this fix.
SELECT count() FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT sum(c0) FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;

-- 2. Different state vs. final type - this is the case that exposed the
-- header mismatch via the nested-Merge `must_return_interpreter_select_query_plan` path.
SELECT uniqExact(c0) FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT round(avg(c0), 4) FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT arraySort(groupArray(c0)) FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;

-- 3. Same aggregates but without the inner Merge - exercises the
-- single-table direct path that the previous test already covered for `count` / `sum`.
SELECT uniqExact(c0) FROM merge(currentDatabase(), '^(t_dist|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT round(avg(c0), 4) FROM merge(currentDatabase(), '^(t_dist|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;

-- 4. GROUP BY with a state-bearing aggregate, with and without the inner Merge.
SELECT c0 % 3 AS g, uniqExact(c0) AS u FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    GROUP BY g ORDER BY g
    SETTINGS distributed_group_by_no_merge = 1;
SELECT c0 % 3 AS g, uniqExact(c0) AS u FROM merge(currentDatabase(), '^(t_dist|t_mem)$')
    GROUP BY g ORDER BY g
    SETTINGS distributed_group_by_no_merge = 1;
-- { echoOff }

DROP TABLE m_inner;
DROP TABLE t_mem;
DROP TABLE t_dist;
DROP TABLE t_mt;
