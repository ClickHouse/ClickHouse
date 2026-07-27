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

-- 1. The reported case: mixed-direction key (a, b DESC), GROUP BY a, b.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_mixed GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_mixed GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);

-- 2. A single reversed column: the defect is not specific to a mixed-direction key.
SELECT
    (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_desc GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_desc GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);

-- 3. A reversed column inside a longer key prefix.
SELECT
    (SELECT groupArray((a, b, sc)) FROM (SELECT a, b, sum(c) AS sc FROM aio_three GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sc)) FROM (SELECT a, b, sum(c) AS sc FROM aio_three GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);

-- 4. Reversed leading column.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_desc_lead GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_desc_lead GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);

-- 5. Negative monotonic match composed with the reverse flags.
SELECT
    (SELECT groupArray((ny, nx, c)) FROM (SELECT negate(y) AS ny, negate(x) AS nx, count() AS c FROM aio_mono GROUP BY negate(y), negate(x) ORDER BY ny, nx) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0)
  = (SELECT groupArray((ny, nx, c)) FROM (SELECT negate(y) AS ny, negate(x) AS nx, count() AS c FROM aio_mono GROUP BY negate(y), negate(x) ORDER BY ny, nx) SETTINGS optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0);

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
-- optimized: the decline is scoped to aggregation only.
SELECT
    (SELECT groupArray(a) FROM (SELECT DISTINCT a FROM aio_differ ORDER BY a) SETTINGS optimize_distinct_in_order = 1)
  = (SELECT groupArray(a) FROM (SELECT DISTINCT a FROM aio_differ ORDER BY a) SETTINGS optimize_distinct_in_order = 0);
SELECT
    (SELECT count() FROM (SELECT a FROM aio_differ LIMIT 1 BY a) SETTINGS optimize_limit_by_in_order = 1)
  = (SELECT count() FROM (SELECT a FROM aio_differ LIMIT 1 BY a) SETTINGS optimize_limit_by_in_order = 0);
SELECT countIf(explain LIKE '%LimitBySortedStreamTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a FROM aio_differ LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1);

-- 7. All-ascending key control: no over-correction.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_asc GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_asc GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);

-- 8. Prefix-only GROUP BY, which never consumed the reversed column: correct before and after.
SELECT
    (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_mixed GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, sb)) FROM (SELECT a, sum(b) AS sb FROM aio_mixed GROUP BY a ORDER BY a) SETTINGS optimize_aggregation_in_order = 0);

-- 9. DISTINCT and LIMIT BY over the reverse key itself: unaffected siblings of the fixed builder.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM aio_mixed ORDER BY a, b) SETTINGS optimize_distinct_in_order = 1)
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM aio_mixed ORDER BY a, b) SETTINGS optimize_distinct_in_order = 0);
SELECT
    (SELECT count() FROM (SELECT a, b FROM aio_mixed LIMIT 1 BY a, b) SETTINGS optimize_limit_by_in_order = 1)
  = (SELECT count() FROM (SELECT a, b FROM aio_mixed LIMIT 1 BY a, b) SETTINGS optimize_limit_by_in_order = 0);

-- 10. ReplacingMergeTree with FINAL over a reverse key is a carrier too.
SELECT
    (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_repl FINAL GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 1)
  = (SELECT groupArray((a, b, sb)) FROM (SELECT a, b, sum(b) AS sb FROM aio_repl FINAL GROUP BY a, b ORDER BY a, b) SETTINGS optimize_aggregation_in_order = 0);

-- Non-vacuity guard: the optimization is really entered for the single-table carrier.
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b FROM aio_mixed GROUP BY a, b SETTINGS optimize_aggregation_in_order = 1);

DROP TABLE aio_mixed;
DROP TABLE aio_desc;
DROP TABLE aio_three;
DROP TABLE aio_desc_lead;
DROP TABLE aio_asc;
DROP TABLE aio_mono;
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
