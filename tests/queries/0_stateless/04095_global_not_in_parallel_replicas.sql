-- Reproduces "Not-ready Set is passed as the second argument" exception that occurs when
-- `buildOrderedSetInplace` is called during primary key analysis for an `IN` subquery and
-- silently fails (e.g. due to subquery timeout with `overflow_mode = 'break'`). On master
-- before the fix, `buildOrderedSetInplace` consumed the source plan up front, so the failure
-- left the set permanently unbuilt and `FunctionIn` threw when the pipeline executed.
-- The fix runs the in-place pipeline against a clone of the source plan, leaving the original
-- intact so that `DelayedCreatingSetsStep::makePlansForSets` can still build the set.
--
-- Observed in stress test with parallel replicas:
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=d0432097aed783bd35054dce2edcefe0c4e5122c&name_0=MasterCI&name_1=Stress%20test%20%28amd_tsan%29
--
-- Tags: replica, no-parallel
-- - no-parallel - global failpoint `prepared_sets_build_ordered_set_inplace_fail`

DROP TABLE IF EXISTS null_in_pr;
CREATE TABLE null_in_pr (dt DateTime, idx Int32, i Nullable(UInt64)) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx;
INSERT INTO null_in_pr SELECT number % 3, number, number FROM system.numbers LIMIT 99999;
INSERT INTO null_in_pr VALUES (0, 123456780, NULL);
INSERT INTO null_in_pr VALUES (1, 123456781, NULL);

SET transform_null_in = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_local_plan = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET use_index_for_in_with_subqueries = 1;

-- Sanity check: queries produce correct results without the failpoint.
SELECT count() == 66668 FROM null_in_pr WHERE i global not in (SELECT i FROM null_in_pr WHERE dt = 2);
SELECT count() == 33333 FROM null_in_pr WHERE i global in (SELECT i FROM null_in_pr WHERE dt = 2);

-- Enable the failpoint that forces `buildOrderedSetInplace` to report failure after the
-- in-place pipeline has run. On master (without the fix) the source plan was consumed up
-- front, so this path left the set permanently unbuilt and triggered
-- "Not-ready Set is passed as the second argument". The fix runs the in-place pipeline on a
-- clone of the source plan, leaving the original available to `makePlansForSets`.
SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- `idx` is the primary key, so the IN subquery triggers primary key analysis and
-- `buildOrderedSetInplace` is actually called here.
SELECT count() > 0 FROM null_in_pr WHERE idx global in (SELECT idx FROM null_in_pr WHERE dt = 2);
SELECT count() >= 0 FROM null_in_pr WHERE idx global not in (SELECT idx FROM null_in_pr WHERE dt = 1);

SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE null_in_pr;
