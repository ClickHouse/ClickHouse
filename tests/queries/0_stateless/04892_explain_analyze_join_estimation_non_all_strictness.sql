-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- The join-order optimizer estimates cardinality, cost, and selectivity in the matched-pair
-- semantics of `ALL` joins, so for `ANY`/`SEMI`/`ANTI` strictness those estimates are wrong by
-- construction (for example, `LEFT SEMI` emits at most one row per left row while the estimate
-- is floored to at least all left rows). For such joins `EXPLAIN` must report the
-- output-derived estimates (`Cost`, `Selectivity`, `Output rows`) as `no stats` and omit
-- `q-error`, while the strictness-independent per-side input row estimates and the measured
-- actual values stay. The data is a 100-row left table with unique `id` and a 300-row right
-- table with ids `0..9` repeated 30 times, so all actual values are deterministic.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10; -- needed for consistent estimated row counts in EXPLAIN output

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
CREATE TABLE table1 (id UInt64, v1 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE table2 (id UInt64, v2 String) ENGINE = MergeTree ORDER BY id;
INSERT INTO table1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO table2 SELECT number % 10, toString(number) FROM numbers(300);

-- LEFT SEMI: 10 left rows match, so the output and the matched output are both 10 rows.
SELECT
    countIf(explain LIKE '%Cost: estimated no stats · actual 10.00%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) no stats%') = 1,
    countIf(explain LIKE '%Output rows: estimated no stats · actual 10.00%') = 1,
    countIf(explain LIKE '%q-error%') = 0,
    countIf(explain LIKE '%Left: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated 300.00 · rows 300.00%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM table1 LEFT SEMI JOIN table2 ON table1.id = table2.id);

-- LEFT ANY: one output row per left row (100). The hash join does not collect per-side
-- matched counters for ANY, so the actual cost and selectivity are `not collected`.
SELECT
    countIf(explain LIKE '%Cost: estimated no stats%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) no stats%') = 1,
    countIf(explain LIKE '%Output rows: estimated no stats · actual 100.00%') = 1,
    countIf(explain LIKE '%q-error%') = 0,
    countIf(explain LIKE '%Left: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated 300.00 · rows 300.00%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM table1 LEFT ANY JOIN table2 ON table1.id = table2.id);

-- LEFT ANTI: 90 unmatched left rows are emitted, none of them is a matched pair, so the
-- actual cost is 0.
SELECT
    countIf(explain LIKE '%Cost: estimated no stats · actual 0.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated no stats · actual 90.00%') = 1,
    countIf(explain LIKE '%q-error%') = 0,
    countIf(explain LIKE '%Left: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated 300.00 · rows 300.00%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM table1 LEFT ANTI JOIN table2 ON table1.id = table2.id);

-- The same gating in EXPLAIN PLAN: the estimation block of a SEMI join reports `no stats`
-- for the output-derived values but keeps the per-side input row estimates.
SELECT
    countIf(explain LIKE '%Cost: estimated no stats%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) no stats%') = 1,
    countIf(explain LIKE '%Output rows: estimated no stats%') = 1,
    countIf(explain LIKE '%Left: rows estimated 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated 300.00%') = 1
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
      SELECT * FROM table1 LEFT SEMI JOIN table2 ON table1.id = table2.id);

-- Control: the same query with ALL strictness keeps the full estimation, so `q-error`
-- is present and nothing reads `no stats`.
SELECT
    countIf(explain LIKE '%q-error%') = 1,
    countIf(explain LIKE '%no stats%') = 0
FROM (EXPLAIN ANALYZE SELECT * FROM table1 LEFT JOIN table2 ON table1.id = table2.id);

DROP TABLE table1;
DROP TABLE table2;
