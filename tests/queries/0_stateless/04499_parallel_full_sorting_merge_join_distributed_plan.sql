-- Tags: long, no-fasttest, no-old-analyzer

-- Regression test: `parallel_full_sorting_merge` must stay compatible with `make_distributed_plan`.
--
-- `optimizeParallelFullSortingMergeJoin` can convert a merge-join `SortingStep` into a scattered full
-- sort (`convertToScatteredFullSort`), which gives it a non-empty `partition_by_description`. Such a
-- step is not serializable for remote execution (`SortingStep::isSerializable`), so if it reached a
-- distributed-plan fragment `convertToDistributed` would reject the query with
-- `... is not serializable for remote execution`.
--
-- A distributed plan keeps its joins logical and physicalizes them per fragment on the worker, so the
-- hash-sharding pass (which matches only a physical `JoinStep`) does not fire while the distributed
-- plan is built; the join runs as a normal distributed merge join. This test checks that building and
-- executing a distributed plan for a `parallel_full_sorting_merge` join succeeds and returns the same
-- result as the single-node `full_sorting_merge` baseline.

DROP TABLE IF EXISTS t_pfsmj_dp_left;
DROP TABLE IF EXISTS t_pfsmj_dp_right;

CREATE TABLE t_pfsmj_dp_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_pfsmj_dp_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Join on `a`/`b` (not the `ORDER BY` key) so a full sort is built for the merge join, exactly the
-- shape the hash-sharding pass targets. Each key value appears 20 times on each side -> 400 matches
-- per value * 1000 values = 400000 rows.
INSERT INTO t_pfsmj_dp_left SELECT number, number % 1000 FROM numbers(20000);
INSERT INTO t_pfsmj_dp_right SELECT number, number % 1000 FROM numbers(20000);

SET enable_analyzer = 1;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET distributed_plan_execute_locally = 1;
-- Force a multi-stage shuffle plan (no broadcast) so the fragment serialization check runs.
SET distributed_plan_default_shuffle_join_bucket_count = 3;
SET distributed_plan_default_reader_bucket_count = 3;
SET distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;

-- Distributed plan with `parallel_full_sorting_merge` must succeed (not throw SUPPORT_IS_DISABLED).
SELECT count() FROM t_pfsmj_dp_left AS l JOIN t_pfsmj_dp_right AS r ON l.a = r.b
SETTINGS make_distributed_plan = 1, join_algorithm = 'parallel_full_sorting_merge';

-- Single-node baseline for correctness.
SELECT count() FROM t_pfsmj_dp_left AS l JOIN t_pfsmj_dp_right AS r ON l.a = r.b
SETTINGS make_distributed_plan = 0, join_algorithm = 'full_sorting_merge';

DROP TABLE t_pfsmj_dp_left;
DROP TABLE t_pfsmj_dp_right;
