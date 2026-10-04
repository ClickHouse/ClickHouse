-- Regression test for issue #112422: JoinResultRowCount omitted the rows emitted by
-- `DelayedJoinedBlocksWorkerTransform`, so any join that spilled to disk under-reported its
-- result size. Results were always correct; only the ProfileEvent was wrong.
--
-- Each cell asserts JoinResultRowCount against result_rows, the number of rows the query
-- actually returned, so a fix that double-counts is just as visible as the original undercount.
-- The cells stream their rows and are read with FORMAT Null: an aggregate such as `count` would
-- log result_rows = 1 and the comparison would be meaningless.
-- The in-memory cell is a control that was already correct before the fix and must not change.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
-- Pin the join orientation: side swapping decides which side emits non-joined rows.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
-- The runner randomizes max_bytes_before_external_join and grace_hash_join_initial_buckets, so
-- every cell pins both explicitly. max_bytes_ratio_before_external_join is not randomized, but its
-- 0.5 default makes the resolved threshold non-zero, which would put the in-memory control on the
-- spill-capable `SpillingHashJoin` path; pinning it to 0 keeps that control a plain `HashJoin`.
SET max_bytes_ratio_before_external_join = 0;
-- The stress profile sets ast_fuzzer_runs=5; a fuzzed re-run inherits log_comment and would win
-- the lookup against system.query_log below.
SET ast_fuzzer_runs = 0;
SET max_threads = 4;

-- In-memory control, INNER: no delayed output, counter already correct before the fix.
SELECT t1.k
FROM (SELECT number AS k FROM numbers(50000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_mem_inner', join_algorithm = 'hash',
    max_bytes_before_external_join = 0, grace_hash_join_initial_buckets = 1
FORMAT Null;

-- Spilling INNER via `SpillingHashJoin`: 50000 matched rows.
SELECT t1.k
FROM (SELECT number AS k FROM numbers(50000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_spill_inner', join_algorithm = 'hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1
FORMAT Null;

-- Spilling LEFT: 25000 matched plus 25000 left rows with no match.
SELECT t1.k
FROM (SELECT number AS k FROM numbers(50000)) AS t1
LEFT JOIN (SELECT number + 25000 AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_spill_left', join_algorithm = 'hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1
FORMAT Null;

-- Spilling RIGHT: right non-joined rows travel through `nextNonJoinedBlock` inside the delayed
-- worker, so this covers a different sub-path than the INNER cell.
SELECT t2.k
FROM (SELECT number + 25000 AS k FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_spill_right', join_algorithm = 'hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1
FORMAT Null;

-- Spilling FULL: 25000 matched, 25000 left-only, 25000 right-only. Non-joined rows from both
-- sides travel through the delayed worker, so this is the widest emitter mix reachable with
-- `join_algorithm = 'hash'`. The runner randomizes parallel_non_joined_rows_processing, so pin
-- it; note it cannot take effect here, because a single-thread `SpillingHashJoin` reports
-- `supportParallelNonJoinedBlocksProcessing` as false and no `NonJoinedBlocksTransform` is
-- built. The parallel_hash cells below are the ones that wire that separate emitter.
SELECT t1.k, t2.k
FROM (SELECT number + 1 AS k FROM numbers(50000)) AS t1
FULL JOIN (SELECT number + 25001 AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_spill_full', join_algorithm = 'hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1,
    parallel_non_joined_rows_processing = 1
FORMAT Null;

-- Explicit grace_hash: a carrier the issue does not mention. It needs at least two initial
-- buckets, because with grace_hash_join_initial_buckets = 1 nothing is delayed and the cell
-- would be vacuous.
SELECT t1.k
FROM (SELECT number AS k FROM numbers(50000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_grace_inner', join_algorithm = 'grace_hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 4
FORMAT Null;

-- The two parallel_hash cells below take the concurrent `SpillingHashJoin` constructor, a
-- different code path from the single-thread one every cell above uses, and they are the only
-- cells here that build the second `DelayedPortsProcessor` of `QueryPipelineBuilder`. They pin
-- that the total stays exact through that extra port layer. The `NonJoinedBlocksTransform`
-- sources this shape also wires stay silent while the join is spilled, because
-- `SpillingHashJoin::isParallelNonJoinedProcessingEnabled` requires the in-memory state, so the
-- delayed worker is still the only emitter of the non-joined rows.

-- parallel_hash RIGHT, spilling: same rows as the 04657_spill_right cell, 50000 in total.
SELECT t2.k
FROM (SELECT number + 25000 AS k FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_ph_right', join_algorithm = 'parallel_hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1,
    parallel_non_joined_rows_processing = 1
FORMAT Null;

-- parallel_hash FULL, spilling: same rows as the 04657_spill_full cell, 75000 in total.
SELECT t1.k, t2.k
FROM (SELECT number + 1 AS k FROM numbers(50000)) AS t1
FULL JOIN (SELECT number + 25001 AS k FROM numbers(50000)) AS t2 ON t1.k = t2.k
SETTINGS log_comment = '04657_ph_full', join_algorithm = 'parallel_hash',
    max_bytes_before_external_join = 100000, grace_hash_join_initial_buckets = 1,
    parallel_non_joined_rows_processing = 1
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- total_equals_result is an equality against ground truth rather than a hardcoded number.
-- has_delayed_rows proves the delayed path was really exercised: without it, a cell that stops
-- spilling would pass while covering nothing.
SELECT
    log_comment,
    ProfileEvents['JoinResultRowCount'] = result_rows AS total_equals_result,
    ProfileEvents['JoinDelayedJoinedTransformRowCount'] > 0 AS has_delayed_rows
FROM system.query_log
WHERE log_comment IN ('04657_mem_inner', '04657_spill_inner', '04657_spill_left',
                      '04657_spill_right', '04657_spill_full', '04657_grace_inner',
                      '04657_ph_right', '04657_ph_full')
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;
