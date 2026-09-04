-- Tags: no-fasttest, no-parallel
-- no-fasttest: needs a build with libfiu to enable the failpoint.
-- no-parallel: the failpoint is server-wide and fires once.

-- `join_algorithm = 'auto'` drains HashJoin onto MergeJoin when `max_rows_in_join`
-- trips. A throw after `releaseJoinedBlocks` must fail the query with the injected
-- error, not leave later fillers inserting into a released HashJoin.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'auto';
SET parallel_hash_join_threshold = 1;
SET max_threads = 8;
SET max_rows_in_join = 50;
SET max_bytes_in_join = 0;

SYSTEM ENABLE FAILPOINT join_switcher_throw_after_hash_release;

SELECT count()
FROM numbers(1000) AS l
INNER JOIN numbers(1000) AS r ON l.number = r.number
FORMAT Null; -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT join_switcher_throw_after_hash_release;

SELECT 'server alive';
