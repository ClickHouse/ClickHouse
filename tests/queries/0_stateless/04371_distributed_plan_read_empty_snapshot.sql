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
-- Pin index_granularity so 100000 rows produce enough marks to split into the 3 requested buckets;
-- otherwise a large granularity yields too few marks, the read is not bucketed, and the assertion below
-- (which requires a bucketed read) does not hold.
CREATE TABLE t_dp_empty_snapshot (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;
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

-- Same for a FINAL read. FINAL resolves each lane's marks to local parts lazily, so an empty local
-- snapshot must be caught by an eager divergence check; otherwise the empty-parts short-circuits
-- (NullSource / total_marks == 0) run first and the worker silently returns zero rows instead of the
-- retryable NO_SUCH_DATA_PART error -- a wrong-results regression the LOGICAL_ERROR fix must not create.
DROP TABLE IF EXISTS t_dp_empty_snapshot_final;
-- Pin index_granularity for the same reason as above: the FINAL read must bucketize so the empty
-- snapshot hits the eager divergence check rather than the non-bucketed zero-row path.
CREATE TABLE t_dp_empty_snapshot_final (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO t_dp_empty_snapshot_final SELECT number, number FROM numbers(100000);

-- Without the fault the bucketed FINAL read works normally.
SELECT count(), sum(v) FROM t_dp_empty_snapshot_final FINAL;

SYSTEM ENABLE FAILPOINT distributed_plan_read_empty_snapshot_on_deserialize;
SELECT count(), sum(v) FROM t_dp_empty_snapshot_final FINAL; -- { serverError NO_SUCH_DATA_PART }
SYSTEM DISABLE FAILPOINT distributed_plan_read_empty_snapshot_on_deserialize;

SELECT count(), sum(v) FROM t_dp_empty_snapshot_final FINAL;

DROP TABLE t_dp_empty_snapshot_final;

-- Non-bucketed read shipped in a distributed fragment: when the distributed axis is an exchange above
-- the read (here a global aggregation over a small FINAL read that is not itself range-split into
-- buckets), the leaf ReadFromMergeTree ships with distributed_read_bucket_count == 0. readFromParts must
-- still build a real (empty) step on an empty local snapshot; returning null crashed the generic plan
-- deserializer with a SIGSEGV. A non-bucketed read is not pinned to coordinator-selected parts, so an
-- empty snapshot is legitimately zero local rows (no divergence contract), not NO_SUCH_DATA_PART.
DROP TABLE IF EXISTS t_dp_empty_snapshot_small;
-- Only 50 rows: one mark regardless of granularity, so this read is never bucketed. Pin the granularity
-- anyway to keep the non-bucketed intent explicit and independent of any injected default.
CREATE TABLE t_dp_empty_snapshot_small (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO t_dp_empty_snapshot_small SELECT number, number FROM numbers(50);

-- Without the fault the non-bucketed FINAL global aggregation works normally.
SELECT count(), sum(v) FROM t_dp_empty_snapshot_small FINAL;

SYSTEM ENABLE FAILPOINT distributed_plan_read_empty_snapshot_on_deserialize;
SELECT count(), sum(v) FROM t_dp_empty_snapshot_small FINAL;
SYSTEM DISABLE FAILPOINT distributed_plan_read_empty_snapshot_on_deserialize;

-- The server is still alive and reads work again once the fault is off.
SELECT count(), sum(v) FROM t_dp_empty_snapshot_small FINAL;

DROP TABLE t_dp_empty_snapshot_small;
