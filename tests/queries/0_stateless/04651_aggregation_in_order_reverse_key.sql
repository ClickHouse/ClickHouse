-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings: optimize_aggregation_in_order is randomized and every assertion pins it per statement
-- no-random-merge-tree-settings: index_granularity and min_bytes_for_wide_part change the part layout,
--   which changes how many groups the pre-fix code collapsed, so the mutation must stay visible
-- no-parallel-replicas: the plan shape and the number of streams differ there

-- Every assertion compares optimize_aggregation_in_order = 1 against the same query at 0 (the oracle),
-- so no result constant is hardcoded. All output lines are 1.

DROP TABLE IF EXISTS aio_mixed;
DROP TABLE IF EXISTS aio_desc;
DROP TABLE IF EXISTS aio_three;
DROP TABLE IF EXISTS aio_desc_lead;
DROP TABLE IF EXISTS aio_asc;
DROP TABLE IF EXISTS aio_mono;
DROP TABLE IF EXISTS aio_mono_float;
DROP TABLE IF EXISTS aio_mono_float_asc;
DROP TABLE IF EXISTS aio_repl;
DROP TABLE IF EXISTS aio_agree_1;
DROP TABLE IF EXISTS aio_agree_2;
DROP TABLE IF EXISTS aio_agree;
DROP TABLE IF EXISTS aio_differ_1;
DROP TABLE IF EXISTS aio_differ_2;
DROP TABLE IF EXISTS aio_differ;
DROP TABLE IF EXISTS aio_merge_asc_1;
DROP TABLE IF EXISTS aio_merge_asc_2;
DROP TABLE IF EXISTS aio_merge_asc;
DROP TABLE IF EXISTS aio_pd;

CREATE TABLE aio_mixed (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b DESC);
SYSTEM STOP MERGES aio_mixed;
INSERT INTO aio_mixed SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_mixed SELECT number % 4, number + 7 FROM numbers(20);

CREATE TABLE aio_desc (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a DESC);
SYSTEM STOP MERGES aio_desc;
INSERT INTO aio_desc SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_desc SELECT number % 4, number + 7 FROM numbers(20);

CREATE TABLE aio_three (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY (a, b DESC, c);
SYSTEM STOP MERGES aio_three;
INSERT INTO aio_three SELECT number % 4, number % 8, number FROM numbers(20);
INSERT INTO aio_three SELECT number % 4, number % 8, number + 7 FROM numbers(20);

CREATE TABLE aio_desc_lead (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a DESC, b);
SYSTEM STOP MERGES aio_desc_lead;
INSERT INTO aio_desc_lead SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_desc_lead SELECT number % 4, number + 7 FROM numbers(20);

CREATE TABLE aio_asc (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);
SYSTEM STOP MERGES aio_asc;
INSERT INTO aio_asc SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_asc SELECT number % 4, number + 7 FROM numbers(20);

-- Int32, so that negate() is recognized as a negative monotonic match of the reversed key columns.
CREATE TABLE aio_mono (x Int32, y Int32) ENGINE = MergeTree ORDER BY (x DESC, y DESC);
SYSTEM STOP MERGES aio_mono;
INSERT INTO aio_mono SELECT number % 4, number % 8 FROM numbers(40);
INSERT INTO aio_mono SELECT number % 4, number % 8 + 3 FROM numbers(40);

-- NaN is a fixed point of negate(), so it keeps its physical position while the advertised
-- direction flips, which is what nulls_direction has to describe.
CREATE TABLE aio_mono_float (x Float64, v UInt64) ENGINE = MergeTree ORDER BY x DESC
SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES aio_mono_float;
INSERT INTO aio_mono_float SELECT if(number % 7 = 0, nan, toFloat64(number % 5)), number FROM numbers(20);
INSERT INTO aio_mono_float SELECT if(number % 3 = 0, nan, toFloat64(number % 5)), number + 7 FROM numbers(20);
INSERT INTO aio_mono_float SELECT if(number % 5 = 0, nan, toFloat64(number % 5)), number + 13 FROM numbers(20);

-- Same shape on a forward key: the null placement is wrong there too, without any reverse flag.
CREATE TABLE aio_mono_float_asc (x Float64, v UInt64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES aio_mono_float_asc;
INSERT INTO aio_mono_float_asc SELECT if(number % 7 = 0, nan, toFloat64(number % 5)), number FROM numbers(20);
INSERT INTO aio_mono_float_asc SELECT if(number % 3 = 0, nan, toFloat64(number % 5)), number + 7 FROM numbers(20);
INSERT INTO aio_mono_float_asc SELECT if(number % 5 = 0, nan, toFloat64(number % 5)), number + 13 FROM numbers(20);

CREATE TABLE aio_repl (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY (a, b DESC);
SYSTEM STOP MERGES aio_repl;
INSERT INTO aio_repl SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_repl SELECT number % 4, number + 7 FROM numbers(20);

-- Merge over children that agree on the physical direction of the matched prefix.
CREATE TABLE aio_agree_1 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC);
CREATE TABLE aio_agree_2 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC);
SYSTEM STOP MERGES aio_agree_1;
SYSTEM STOP MERGES aio_agree_2;
INSERT INTO aio_agree_1 SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_agree_2 SELECT number % 4, number + 5 FROM numbers(20);
CREATE TABLE aio_agree (a UInt32, v UInt32) ENGINE = Merge(currentDatabase(), '^aio_agree_[12]$');

-- Merge over children that disagree on the physical direction of the matched prefix.
CREATE TABLE aio_differ_1 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a);
CREATE TABLE aio_differ_2 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC);
SYSTEM STOP MERGES aio_differ_1;
SYSTEM STOP MERGES aio_differ_2;
INSERT INTO aio_differ_1 SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_differ_2 SELECT number % 4, number + 5 FROM numbers(20);
CREATE TABLE aio_differ (a UInt32, v UInt32) ENGINE = Merge(currentDatabase(), '^aio_differ_[12]$');

-- All-ascending Merge control.
CREATE TABLE aio_merge_asc_1 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a);
CREATE TABLE aio_merge_asc_2 (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a);
SYSTEM STOP MERGES aio_merge_asc_1;
SYSTEM STOP MERGES aio_merge_asc_2;
INSERT INTO aio_merge_asc_1 SELECT number % 4, number FROM numbers(20);
INSERT INTO aio_merge_asc_2 SELECT number % 4, number + 5 FROM numbers(20);
CREATE TABLE aio_merge_asc (a UInt32, v UInt32) ENGINE = Merge(currentDatabase(), '^aio_merge_asc_[12]$');

-- Reversed key with enough granules for the LIMIT push-down read_rows oracle of case 11.
CREATE TABLE aio_pd (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k DESC) SETTINGS index_granularity = 16;
INSERT INTO aio_pd SELECT number % 100 AS k, number AS v FROM numbers(1000) ORDER BY k DESC;

-- 1. The reported case: mixed-direction key (a, b DESC), GROUP BY a, b.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_mixed GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_mixed GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);

-- Each carrier below pairs its oracle comparison with an engagement assertion. The oracle alone is
-- blind to the optimization silently not being applied: both sides would then take the fallback path
-- and compare equal. The engagement line pins eligibility, so it must hold on a pre-fix binary too.

-- 2. A single reversed column: the defect is not specific to a mixed-direction key.
SELECT
    (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_desc GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_desc GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM aio_desc GROUP BY a SETTINGS optimize_aggregation_in_order = 1);

-- 3. A reversed column inside a longer key prefix.
SELECT
    (SELECT groupArray((a, b, sc)) FROM (SELECT a, b, sum(c) AS sc FROM aio_three GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sc)) FROM (SELECT a, b, sum(c) AS sc FROM aio_three GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b, sum(c) FROM aio_three GROUP BY a, b SETTINGS optimize_aggregation_in_order = 1);

-- 4. Reversed leading column.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_desc_lead GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_desc_lead GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b, sum(b) FROM aio_desc_lead GROUP BY a, b SETTINGS optimize_aggregation_in_order = 1);

-- 4b. Reversed leading column, grouped by a strict prefix of the key. This shape collapses to a
-- single group for the whole table, unlike case 4, which keeps one row per key tuple. The group
-- count is asserted on its own line because the groupArray oracle compares equal for a collapse and
-- for a reordering alike, so it does not record which of the two occurred.
SELECT
    (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_desc_lead GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_desc_lead GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT
    (SELECT count() FROM (SELECT a FROM aio_desc_lead GROUP BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT count() FROM (SELECT a FROM aio_desc_lead GROUP BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM aio_desc_lead GROUP BY a SETTINGS optimize_aggregation_in_order = 1);

-- 5. Negative monotonic match composed with the reverse flags.
-- optimize_injective_functions_in_group_by = 0 is load-bearing on both lines: otherwise the analyzer
-- rewrites the keys and the monotonicity branch of the builder is never taken.
SELECT
    (SELECT groupArray((ny, nx, c)) FROM (SELECT negate(y) AS ny, negate(x) AS nx, count() AS c FROM aio_mono GROUP BY negate(y), negate(x) ORDER BY ny, nx) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0)
  = (SELECT groupArray((ny, nx, c)) FROM (SELECT negate(y) AS ny, negate(x) AS nx, count() AS c FROM aio_mono GROUP BY negate(y), negate(x) ORDER BY ny, nx) SETTINGS optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT negate(y), negate(x), count() FROM aio_mono GROUP BY negate(y), negate(x) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0);

-- 5b. Same over a Float reversed key, where NaN plays the role of NULL.
SELECT
    (SELECT groupArray((n, c, s)) FROM (SELECT negate(x) AS n, count() AS c, sum(v) AS s FROM aio_mono_float GROUP BY n ORDER BY isNaN(n) DESC, n) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0)
  = (SELECT groupArray((n, c, s)) FROM (SELECT negate(x) AS n, count() AS c, sum(v) AS s FROM aio_mono_float GROUP BY n ORDER BY isNaN(n) DESC, n) SETTINGS optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0);
SELECT
    (SELECT count() FROM (SELECT negate(x) AS n FROM aio_mono_float GROUP BY n) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0)
  = (SELECT count() FROM (SELECT negate(x) AS n FROM aio_mono_float GROUP BY n) SETTINGS optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT negate(x), count() FROM aio_mono_float GROUP BY negate(x) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0);

-- 5c. Forward key, no reverse flag: the same null placement is required, so this case fails on
-- master as well as on the reverse-flag fix alone.
SELECT
    (SELECT groupArray((n, c, s)) FROM (SELECT negate(x) AS n, count() AS c, sum(v) AS s FROM aio_mono_float_asc GROUP BY n ORDER BY isNaN(n) DESC, n) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0)
  = (SELECT groupArray((n, c, s)) FROM (SELECT negate(x) AS n, count() AS c, sum(v) AS s FROM aio_mono_float_asc GROUP BY n ORDER BY isNaN(n) DESC, n) SETTINGS optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0);
SELECT
    (SELECT count() FROM (SELECT negate(x) AS n FROM aio_mono_float_asc GROUP BY n) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0)
  = (SELECT count() FROM (SELECT negate(x) AS n FROM aio_mono_float_asc GROUP BY n) SETTINGS optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT negate(x), count() FROM aio_mono_float_asc GROUP BY negate(x) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0);

-- 6a. Merge whose children agree on the direction: fixed, and the optimization is KEPT.
SELECT
    (SELECT groupArray((a, sv)) FROM (SELECT a, sum(v) AS sv FROM aio_agree GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sv)) FROM (SELECT a, sum(v) AS sv FROM aio_agree GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, sum(v) FROM aio_agree GROUP BY a SETTINGS optimize_aggregation_in_order = 1);

-- 6b. Merge whose children disagree on the direction: no single direction can describe both streams,
-- so aggregation in order is declined. The inverse plan assertion is what pins the decline.
SELECT
    (SELECT groupArray((a, sv)) FROM (SELECT a, sum(v) AS sv FROM aio_differ GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sv)) FROM (SELECT a, sum(v) AS sv FROM aio_differ GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') = 0 FROM (EXPLAIN PIPELINE SELECT a, sum(v) FROM aio_differ GROUP BY a SETTINGS optimize_aggregation_in_order = 1);

-- 6c. All-ascending Merge control: the decline must not widen to it.
SELECT
    (SELECT groupArray((a, sv)) FROM (SELECT a, sum(v) AS sv FROM aio_merge_asc GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sv)) FROM (SELECT a, sum(v) AS sv FROM aio_merge_asc GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, sum(v) FROM aio_merge_asc GROUP BY a SETTINGS optimize_aggregation_in_order = 1);

-- 6d. DISTINCT and LIMIT BY over the same mixed-direction Merge are correct today and must stay
-- optimized: the decline is scoped to aggregation only. The LimitBySortedStreamTransform assertion
-- below is also what keeps 6b honest -- it proves this Merge IS eligible for read-in-order, so 6b's
-- "= 0" really means "declined by the new guard" and not "never eligible at all". Do not delete it
-- as redundant sibling coverage.
SELECT
    (SELECT groupArray(a) FROM (SELECT DISTINCT a FROM aio_differ ORDER BY a) SETTINGS optimize_distinct_in_order = 1)
  = (SELECT groupArray(a) FROM (SELECT DISTINCT a FROM aio_differ ORDER BY a) SETTINGS optimize_distinct_in_order = 0);
SELECT countIf(explain LIKE '%DistinctSortedStreamTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a FROM aio_differ SETTINGS optimize_distinct_in_order = 1);
SELECT
    (SELECT count() FROM (SELECT a FROM aio_differ LIMIT 1 BY a) SETTINGS optimize_limit_by_in_order = 1)
  = (SELECT count() FROM (SELECT a FROM aio_differ LIMIT 1 BY a) SETTINGS optimize_limit_by_in_order = 0);
SELECT countIf(explain LIKE '%LimitBySortedStreamTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a FROM aio_differ LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1);

-- 7. All-ascending key control: no over-correction.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_asc GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_asc GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b, sum(b) FROM aio_asc GROUP BY a, b SETTINGS optimize_aggregation_in_order = 1);

-- 8. Prefix-only GROUP BY, which never consumed the reversed column: correct before and after.
SELECT
    (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_mixed GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_mixed GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM aio_mixed GROUP BY a SETTINGS optimize_aggregation_in_order = 1);

-- 9. DISTINCT and LIMIT BY over the reverse key itself: unaffected siblings of the fixed builder.
-- Result equality alone would hold whether or not they are optimized, so it cannot detect the
-- over-correction this control exists to detect; the two engagement lines are what make it a control.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM aio_mixed ORDER BY a, b) SETTINGS optimize_distinct_in_order = 1)
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM aio_mixed ORDER BY a, b) SETTINGS optimize_distinct_in_order = 0);
SELECT countIf(explain LIKE '%DistinctSortedStreamTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM aio_mixed SETTINGS optimize_distinct_in_order = 1);
SELECT
    (SELECT count() FROM (SELECT a, b FROM aio_mixed LIMIT 1 BY a, b) SETTINGS optimize_limit_by_in_order = 1)
  = (SELECT count() FROM (SELECT a, b FROM aio_mixed LIMIT 1 BY a, b) SETTINGS optimize_limit_by_in_order = 0);
SELECT countIf(explain LIKE '%LimitBySortedStreamTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b FROM aio_mixed LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1);

-- 10. ReplacingMergeTree with FINAL over a reverse key is a carrier too.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_repl FINAL GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_repl FINAL GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b, sum(b) FROM aio_repl FINAL GROUP BY a, b SETTINGS optimize_aggregation_in_order = 1);

-- Non-vacuity guard: the optimization is really entered for the single-table carrier.
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b FROM aio_mixed GROUP BY a, b SETTINGS optimize_aggregation_in_order = 1);

-- 11. LIMIT push-down into aggregation in order over a reversed key.
-- optimizeLimitForAggregationInOrder matches the LimitStep's sort description against the
-- aggregation's group-by description with SortDescription::hasPrefix, and SortColumnDescription's
-- operator== compares `direction`. With the pre-fix sign the group-by description read `k ASC` while
-- the ORDER BY reads `k DESC`, so it did not match and the limit was not pushed into the aggregator;
-- it now matches, so such queries additionally gain early termination.
-- NULLS FIRST is load-bearing: operator== also compares `nulls_direction`, the group-by description
-- leaves it at its default +1, and a bare `ORDER BY k DESC` yields -1 (NULLS FIRST puts it back to
-- +1), so without it the descriptions would never match regardless of the direction fix.
-- The small-block settings expose the effect on this 1000-row table; enable_parallel_replicas = 0
-- because read_rows accounting differs when the reads happen on remote replicas.
SELECT k, count() FROM aio_pd GROUP BY k ORDER BY k DESC NULLS FIRST LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 1, max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0, log_comment = '04651_pushdown_on' FORMAT Null;

SELECT k, count() FROM aio_pd GROUP BY k ORDER BY k DESC NULLS FIRST LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0,
         max_threads = 1, max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0, log_comment = '04651_pushdown_off' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT on_reads < off_reads
FROM (
    SELECT
        anyIf(read_rows, log_comment = '04651_pushdown_on') AS on_reads,
        anyIf(read_rows, log_comment = '04651_pushdown_off') AS off_reads
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04651_pushdown_on', '04651_pushdown_off')
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND event_time >= now() - 600
);

DROP TABLE aio_mixed;
DROP TABLE aio_desc;
DROP TABLE aio_three;
DROP TABLE aio_desc_lead;
DROP TABLE aio_asc;
DROP TABLE aio_mono;
DROP TABLE aio_mono_float;
DROP TABLE aio_mono_float_asc;
DROP TABLE aio_repl;
DROP TABLE aio_agree;
DROP TABLE aio_agree_1;
DROP TABLE aio_agree_2;
DROP TABLE aio_differ;
DROP TABLE aio_differ_1;
DROP TABLE aio_differ_2;
DROP TABLE aio_merge_asc;
DROP TABLE aio_merge_asc_1;
DROP TABLE aio_merge_asc_2;
DROP TABLE aio_pd;
