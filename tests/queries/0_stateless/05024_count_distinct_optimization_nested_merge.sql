-- Tags: distributed

-- `count_distinct_optimization` must not break reading from a `Merge` table that mixes a
-- remote carrier (an inner `Merge` over a `Distributed` table) with a plain local table.
-- The rewrite is skipped for remote carriers at the top level, and the per-child plans
-- built by `ReadFromMerge` must not re-run the rewrite either: a child plan producing
-- `count()` while the common header expects an `AggregateFunction(uniqExact, ...)` state
-- used to throw `CANNOT_CONVERT_TYPE`.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS m_inner;

CREATE TABLE t_mt (c0 Int) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_mt SELECT * FROM numbers(10);

CREATE TABLE t_dist (c0 Int) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_mt');

CREATE TABLE t_mem (c0 Int) ENGINE = Memory;
INSERT INTO t_mem SELECT * FROM numbers(10);

CREATE TABLE m_inner (c0 Int) ENGINE = Merge(currentDatabase(), '^t_dist$');

SET count_distinct_optimization = 1;

-- { echoOn }
SELECT uniqExact(c0) FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT uniqExact(c0) FROM merge(currentDatabase(), '^(m_inner|t_mem)$');
SELECT countDistinct(c0) FROM merge(currentDatabase(), '^(m_inner|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT uniqExact(c0) FROM merge(currentDatabase(), '^(t_dist|t_mem)$')
    SETTINGS distributed_group_by_no_merge = 1;
SELECT uniqExact(c0) FROM merge(currentDatabase(), '^t_mem$');
-- { echoOff }

DROP TABLE m_inner;
DROP TABLE t_mem;
DROP TABLE t_dist;
DROP TABLE t_mt;
