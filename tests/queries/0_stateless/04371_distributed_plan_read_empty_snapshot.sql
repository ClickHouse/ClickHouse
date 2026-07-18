-- Tags: no-parallel, no-old-analyzer
-- no-parallel: enables a global failpoint that would disrupt other distributed-plan queries.
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for the LOGICAL_ERROR "ReadFromMergeTree step is expected to be created by
-- readFromParts" in ReadFromMergeTree::deserialize. A worker deserializes a bucketed distributed
-- read whose local parts snapshot has become empty (parts merged away or dropped since the
-- coordinator planned the read). readFromParts used to return a null step for an empty non-stream
-- read, and the distributed_read_bucket_count branch then dynamic_cast-ed it to null and threw a
-- LOGICAL_ERROR (server abort in debug builds). It must instead build an empty read and let the
-- pipeline resolve it -- here to the retryable NO_SUCH_DATA_PART divergence error.

DROP TABLE IF EXISTS t_dp_empty_snapshot;
CREATE TABLE t_dp_empty_snapshot (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dp_empty_snapshot SELECT number FROM numbers(100000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_max_rows_to_broadcast = 0,
    max_rows_to_group_by = 0;

-- Without the fault the bucketed read works normally.
SELECT sum(x) FROM t_dp_empty_snapshot;

-- The failpoint makes the deserialize side see an empty local snapshot. Before the fix this aborted
-- the server with a LOGICAL_ERROR; now it is the ordinary retryable divergence error.
SYSTEM ENABLE FAILPOINT distributed_plan_read_empty_snapshot_on_deserialize;
SELECT sum(x) FROM t_dp_empty_snapshot; -- { serverError NO_SUCH_DATA_PART }
SYSTEM DISABLE FAILPOINT distributed_plan_read_empty_snapshot_on_deserialize;

-- The server is still alive and reads work again once the fault is off.
SELECT sum(x) FROM t_dp_empty_snapshot;

DROP TABLE t_dp_empty_snapshot;
