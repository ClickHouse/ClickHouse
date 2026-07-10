-- Tags: no-parallel-replicas, no-old-analyzer, no-darwin
-- no-old-analyzer: the optimization requires the analyzer.
-- no-darwin: STREAM reads are Linux-only.

------------------------------------------------------------------------------------------------
-- Output-only correctness. Only the setting under test is pinned; everything else is left to
-- the test harness's settings randomization so that CI exercises arbitrary combinations. The
-- results must be correct whether or not the rewrite fires, so no plan shape is checked here.
------------------------------------------------------------------------------------------------

SET query_plan_optimize_self_join_shared_scan = 1;

DROP TABLE IF EXISTS t_sjss_out;
CREATE TABLE t_sjss_out (x UInt64, y String, z UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_out SELECT number, toString(number), number % 7 FROM numbers(1000);
INSERT INTO t_sjss_out SELECT number, toString(number), number % 5 FROM numbers(1000, 500);

-- INNER self-join on the primary key.
SELECT count(), sum(a.x), sum(b.z) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x;

-- LEFT self-join on a non-unique column (fan-out).
SELECT count(), sum(a.x), sum(b.x) FROM t_sjss_out AS a LEFT JOIN t_sjss_out AS b ON a.z = b.z;

-- The probe side reads a strict subset of the build side's columns.
SELECT count(), max(b.y) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x;

-- Expressions between the scans and the join.
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x + 1 = b.x;

-- A filter on the probe side.
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x WHERE a.z = 3;

-- Aggregation on top of a fan-out join.
SELECT sum(cnt), max(cnt) FROM (SELECT a.x, count() AS cnt FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.z = b.z GROUP BY a.x);

-- Three-way self-join.
SELECT count(), sum(c.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x INNER JOIN t_sjss_out AS c ON b.x = c.x;

-- Explicit algorithm choices, including ones the rewrite is incompatible with.
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'grace_hash';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'auto';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'full_sorting_merge,hash';
SELECT count(), sum(b.x) FROM t_sjss_out AS a INNER JOIN t_sjss_out AS b ON a.x = b.x SETTINGS join_algorithm = 'hash,full_sorting_merge';

DROP TABLE t_sjss_out;

------------------------------------------------------------------------------------------------
-- Plan-shape checks. These require the rewrite to fire (or not fire) deterministically, so the
-- settings that the test harness randomizes and that each independently change the plan are
-- pinned from here on: `enable_join_runtime_filters` adds a filter to the probe scan,
-- `enable_parallel_replicas` and `enable_shared_storage_snapshot_in_query = 0` disable the
-- rewrite.
------------------------------------------------------------------------------------------------

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_shared_storage_snapshot_in_query = 1;
-- Pin the join order: a swapped self-join changes which side's columns must be a subset of the
-- other's, so whether the rewrite fires.
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_sjss;
CREATE TABLE t_sjss (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss SELECT number, toString(number) FROM numbers(10);

-- Correctness with optimization on.
SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x ORDER BY a.x;

-- Same query without optimization, results must match.
SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x ORDER BY a.x SETTINGS query_plan_optimize_self_join_shared_scan = 0;

-- Plan shape: single scan, save buffer, read buffer.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
);

-- Negative: FINAL must NOT trigger optimization (still 2 ReadFromMergeTree).
DROP TABLE IF EXISTS t_sjss_rmt;
CREATE TABLE t_sjss_rmt (x UInt64, y String) ENGINE = ReplacingMergeTree ORDER BY x;
INSERT INTO t_sjss_rmt SELECT number, toString(number) FROM numbers(10);

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_rmt AS a FINAL INNER JOIN t_sjss_rmt AS b ON a.x = b.x
);

-- Negative: Different tables must NOT trigger optimization.
DROP TABLE IF EXISTS t_sjss2;
CREATE TABLE t_sjss2 (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss2 SELECT number, toString(number) FROM numbers(10);

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss2 AS b ON a.x = b.x
);

-- LEFT JOIN should also fire.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a LEFT JOIN t_sjss AS b ON a.x = b.x
);

-- Non-hash algorithm (full_sorting_merge) must NOT trigger optimization,
-- and must not error out with "Can't execute any of specified join algorithms".
SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'full_sorting_merge';

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
    SETTINGS join_algorithm = 'full_sorting_merge'
);

-- Setting off must keep two scans.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
    SETTINGS query_plan_optimize_self_join_shared_scan = 0
);

-- Negative: non-shared snapshot must NOT trigger optimization (2 scans, no buffer).
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
    SETTINGS enable_shared_storage_snapshot_in_query = 0
);

DROP TABLE t_sjss;
DROP TABLE t_sjss2;
DROP TABLE t_sjss_rmt;

------------------------------------------------------------------------------------------------
-- Mixed algorithm lists. `chooseJoinAlgorithm` walks `join_algorithm` in order and executes the
-- first algorithm that applies. The rewrite must not change which algorithm wins: with a
-- merge-style algorithm listed before a hash one the merge-style join is executed, so the
-- rewrite must NOT fire (2 scans).
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS t_sjss_mixed;
CREATE TABLE t_sjss_mixed (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_mixed SELECT number, toString(number) FROM numbers(10);

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'full_sorting_merge,hash'
);

-- Correctness of the untouched merge-style join.
SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'full_sorting_merge,hash';

-- Same with a partial-merge algorithm before a hash-family one.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'prefer_partial_merge,grace_hash'
);

-- grace_hash falls through to the next entry when it does not support the join, so a merge-style
-- fallback after it must also prevent the rewrite.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'grace_hash,full_sorting_merge'
);

-- A hash algorithm listed first always wins, so a later merge-style entry is unreachable and the
-- rewrite fires (1 scan + buffer replay).
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'hash,full_sorting_merge'
);

SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'hash,full_sorting_merge';

-- The deprecated `default` means `direct,hash`; direct never applies to a MergeTree build side,
-- so hash wins and the rewrite fires.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_mixed AS a INNER JOIN t_sjss_mixed AS b ON a.x = b.x
    SETTINGS join_algorithm = 'default'
);

DROP TABLE t_sjss_mixed;

------------------------------------------------------------------------------------------------
-- grace_hash. It uses the producer-first pipeline (build side fully consumed before the probe
-- side is read), so it is compatible with the shared-scan rewrite, which must keep firing for a
-- user-requested on-disk algorithm.
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS t_sjss_gh;
CREATE TABLE t_sjss_gh (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_gh SELECT number, toString(number) FROM numbers(10);

-- Correctness with grace_hash requested first.
SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'grace_hash,hash';

-- Same query without the rewrite, results must match.
SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'grace_hash,hash', query_plan_optimize_self_join_shared_scan = 0;

-- The rewrite must fire with grace_hash alone.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x
    SETTINGS join_algorithm = 'grace_hash'
);

-- Correctness with grace_hash alone.
SELECT a.x, b.y FROM t_sjss_gh AS a INNER JOIN t_sjss_gh AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'grace_hash';

DROP TABLE t_sjss_gh;

------------------------------------------------------------------------------------------------
-- auto. It uses the producer-first pipeline (it resolves to `SpillingHashJoin`, `JoinSwitcher`,
-- or `HashJoin`), so it is compatible with the shared-scan rewrite: the rewrite must fire and
-- must not disturb a user-requested `auto`, or the configured under-memory-pressure fallback
-- (spill to disk or switch to merge join) would be silently replaced with an exception.
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS t_sjss_auto;
CREATE TABLE t_sjss_auto (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_auto SELECT number, toString(number) FROM numbers(10);

-- Correctness with auto requested first.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto,hash';

-- Same query without the rewrite, results must match.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto,hash', query_plan_optimize_self_join_shared_scan = 0;

-- The rewrite must fire with auto alone.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x
    SETTINGS join_algorithm = 'auto'
);

-- Correctness with auto alone.
SELECT a.x, b.y FROM t_sjss_auto AS a INNER JOIN t_sjss_auto AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'auto';

DROP TABLE t_sjss_auto;

------------------------------------------------------------------------------------------------
-- STREAM scans. A `STREAM` scan is unbounded and keeps producing newly committed rows, so the
-- rewrite must not buffer it or replay it from a one-shot buffer. Every mix of `STREAM` and
-- plain scans must keep two `ReadFromMergeTree`.
------------------------------------------------------------------------------------------------

SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_sjss_stream;
CREATE TABLE t_sjss_stream (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_stream SELECT number, toString(number) FROM numbers(10);

-- STREAM on the probe (left) side.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_stream AS a STREAM INNER JOIN t_sjss_stream AS b ON a.x = b.x
);

-- STREAM on the build (right) side.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_stream AS a INNER JOIN t_sjss_stream AS b STREAM ON a.x = b.x
);

-- STREAM on both sides.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_stream AS a STREAM INNER JOIN t_sjss_stream AS b STREAM ON a.x = b.x
);

DROP TABLE t_sjss_stream;

------------------------------------------------------------------------------------------------
-- Read-in-order (through join). The rewrite runs before `optimizeReadInOrder`, so it never
-- observes an in-order reading contract, and the explicit ORDER BY sort keeps results correct
-- whether or not it fires.
------------------------------------------------------------------------------------------------

SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

DROP TABLE IF EXISTS t_sjss_rio;
CREATE TABLE t_sjss_rio (t UInt64, id UInt64) ENGINE = MergeTree ORDER BY t;
INSERT INTO t_sjss_rio SELECT number, number % 8 FROM numbers(1000);

-- The rewrite fires (left columns are a subset of right columns), the ORDER BY is on the sorting
-- key and the join is on a different column: results must be correct.
SELECT a.t, a.id, b.t FROM t_sjss_rio AS a INNER JOIN t_sjss_rio AS b ON a.id = b.id
ORDER BY a.t, b.t LIMIT 10;

-- Same query with the optimization off, results must match.
SELECT a.t, a.id, b.t FROM t_sjss_rio AS a INNER JOIN t_sjss_rio AS b ON a.id = b.id
ORDER BY a.t, b.t LIMIT 10
SETTINGS query_plan_optimize_self_join_shared_scan = 0;

-- Plan shape: one shared scan feeds a buffer, and an explicit Sorting step preserves ORDER BY.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count,
    countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%') AS sort_count
FROM (
    EXPLAIN actions = 0
    SELECT a.t, a.id, b.t FROM t_sjss_rio AS a INNER JOIN t_sjss_rio AS b ON a.id = b.id
    ORDER BY a.t, b.t LIMIT 10
);

DROP TABLE t_sjss_rio;

------------------------------------------------------------------------------------------------
-- join_overflow_mode = 'break'. The build side may stop consuming its input when
-- `max_rows_in_join` / `max_bytes_in_join` is reached, so the shared buffer would hold only a
-- prefix of the scan and the probe side would lose rows beyond the join's soft limit
-- (e.g. the preserved side of a LEFT JOIN). The rewrite must not fire in that case.
------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS t_sjss_break;
CREATE TABLE t_sjss_break (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_break SELECT number, toString(number) FROM numbers(100);

-- Plan shape: `join_overflow_mode = 'break'` with a row limit keeps two scans and no buffer.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_rows_in_join = 10, join_overflow_mode = 'break'
);

-- Same with a byte limit.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break'
);

-- Every left-side row must survive: the LEFT JOIN's soft limit may only drop matches,
-- never rows of the preserved side. The small `max_block_size` splits the scan into many
-- blocks so the build side actually stops early at the limit.
SELECT count() FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
SETTINGS max_rows_in_join = 10, join_overflow_mode = 'break', max_block_size = 10;

-- `join_overflow_mode = 'throw'` (the default) with a limit still allows the rewrite.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_rows_in_join = 1000, join_overflow_mode = 'throw'
);

-- 'break' without any limit set is inert, the rewrite may fire.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_break AS a LEFT JOIN t_sjss_break AS b ON a.x = b.x
    SETTINGS max_rows_in_join = 0, max_bytes_in_join = 0, join_overflow_mode = 'break'
);

DROP TABLE t_sjss_break;

------------------------------------------------------------------------------------------------
-- Distributed plans. The rewrite introduces steps that do not support plan serialization
-- (`CommonSubplanStep` / `CommonSubplanReferenceStep` and the buffer steps they are lowered to),
-- so it must not fire under `make_distributed_plan`: previously a qualifying self-join failed
-- with an exception from `assertFragmentSerializable` instead of skipping the optimization.
------------------------------------------------------------------------------------------------

SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_sjss_dist;
CREATE TABLE t_sjss_dist (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_dist SELECT number, toString(number) FROM numbers(10);

-- Must not fail and must return correct results.
SELECT a.x, b.y FROM t_sjss_dist AS a INNER JOIN t_sjss_dist AS b ON a.x = b.x ORDER BY a.x
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- Plan shape: the rewrite must not fire, so no shared buffer appears in the distributed plan.
-- `make_distributed_plan` is set only on the inner query: the outer wrapper reads from the
-- EXPLAIN storage, which itself cannot be distributed.
SELECT countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_dist AS a INNER JOIN t_sjss_dist AS b ON a.x = b.x
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1
);

DROP TABLE t_sjss_dist;
