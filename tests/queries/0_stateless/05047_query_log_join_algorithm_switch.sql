-- `used_join_algorithms` of `system.query_log` reports the algorithm that ran and not the one the
-- pipeline was built with, so a join that changes its algorithm mid-flight has to report both. This
-- is the `JoinSwitcher` transition: under `join_algorithm = 'auto'` the pipeline is built with a
-- `HashJoin` and the right table is handed to a `MergeJoin` as soon as it outgrows the limit, which
-- is the only path that reports `PARTIAL_MERGE` on top of the `HASH` the pipeline was built with.
-- The `grace_hash` transition of the same column is covered by 04891_query_log_join_columns.sql.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides
-- swapped, which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;
-- Both of these have to be off. As long as a hash join is allowed to spill, `auto` builds a
-- `SpillingHashJoin` instead, which never hands the right table over to a `MergeJoin`, so the switch
-- under test never happens. The ratio has to be named too, and not only the byte threshold, because
-- it derives a non-zero threshold of its own.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT 'hash switching to partial merge';
-- The right table of 4000 rows crosses `max_rows_in_join` while it is being built, so the join that
-- started as HASH finishes as PARTIAL_MERGE and both are reported for the single join. The switch is
-- also what keeps the query alive: the same limit makes the plain hash join below fail.
SELECT count() FROM (SELECT number AS a FROM numbers(4)) t1
JOIN (SELECT number AS a FROM numbers(4000)) t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05047_join_switch_auto', join_algorithm = 'auto', max_rows_in_join = 1000;

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment = '05047_join_switch_auto';

SELECT 'hash without the switch';
-- The same query and the same limit, with the algorithm pinned to `hash`: nothing takes the right
-- table over, so the limit is fatal and the row of the failed query reports HASH alone. This is the
-- contrast that shows the PARTIAL_MERGE above comes from the run time switch and not from the
-- pipeline that was built.
SELECT count() FROM (SELECT number AS a FROM numbers(4)) t1
JOIN (SELECT number AS a FROM numbers(4000)) t2 ON t1.a = t2.a
FORMAT Null
SETTINGS log_comment = '05047_join_switch_hash', join_algorithm = 'hash', max_rows_in_join = 1000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;
SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'ExceptionWhileProcessing'
  AND event_date >= yesterday()
  AND log_comment = '05047_join_switch_hash';
