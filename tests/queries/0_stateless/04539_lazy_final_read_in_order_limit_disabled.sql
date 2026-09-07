-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local query plan.

DROP TABLE IF EXISTS t_lazy_final_gates;

CREATE TABLE t_lazy_final_gates (k UInt64, v UInt64, s String) ENGINE = ReplacingMergeTree ORDER BY k;

SYSTEM STOP MERGES t_lazy_final_gates;

INSERT INTO t_lazy_final_gates SELECT number, if(number < 20000, 7, 999), toString(number) FROM numbers(100000);
INSERT INTO t_lazy_final_gates SELECT number, if(number < 20000, 7, 999), 'updated' FROM numbers(100000);

SET enable_analyzer = 1, query_plan_optimize_lazy_final = 1, optimize_read_in_order = 1, max_threads = 4;
SET exact_rows_before_limit = 0;

-- Filter without ORDER BY and LIMIT: lazy FINAL applies.
SELECT 'no order, no limit:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7);

-- Read-in-order: lazy FINAL must be disabled (the replacement plan does not produce rows in sorting key order).
SELECT 'read-in-order:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7 ORDER BY k);

SELECT 'read-in-order, limit:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7 ORDER BY k LIMIT 10);

-- A small LIMIT without ORDER BY stops reading early: lazy FINAL must be disabled.
SELECT 'small limit:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7 LIMIT 10);

-- A LIMIT not smaller than the number of selected rows cannot stop reading early: lazy FINAL applies.
SELECT 'large limit:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7 LIMIT 1000000);

-- With exact_rows_before_limit the limit reads the whole stream anyway: lazy FINAL applies.
SELECT 'limit reading till end:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7 LIMIT 10 SETTINGS exact_rows_before_limit = 1);

-- A LIMIT above a full sort consumes the whole stream: lazy FINAL applies.
SELECT 'limit above full sort:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates FINAL WHERE v = 7 ORDER BY s LIMIT 10);

-- DISTINCT with a pushed-down limit hint stops reading early: lazy FINAL must be disabled.
SELECT 'distinct with limit:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT DISTINCT k % 10 FROM t_lazy_final_gates FINAL WHERE v = 7 LIMIT 5);

-- DISTINCT without a limit consumes the whole stream: lazy FINAL applies.
SELECT 'distinct without limit:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT DISTINCT k % 10 FROM t_lazy_final_gates FINAL WHERE v = 7);

-- The DISTINCT transform stops reading at its limit hint even when the enclosing limit
-- has always_read_till_end (exact_rows_before_limit), so lazy FINAL must be disabled
-- here the same way as without the setting.
SELECT 'distinct with limit reading till end:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT DISTINCT k % 10 FROM t_lazy_final_gates FINAL WHERE v = 7 LIMIT 5 SETTINGS exact_rows_before_limit = 1);

-- arrayJoin changes the number of rows, so a limit above it is not comparable to the
-- number of selected rows: lazy FINAL applies.
SELECT 'limit above arrayJoin:', countIf(explain LIKE '%InputSelector%') > 0
FROM (EXPLAIN SELECT arrayJoin([k, k]) FROM t_lazy_final_gates FINAL WHERE v = 7 LIMIT 10);

-- The results must be the same with and without the optimization.
SELECT k, v, s FROM t_lazy_final_gates FINAL WHERE v = 7 ORDER BY k LIMIT 5;
SELECT k, v, s FROM t_lazy_final_gates FINAL WHERE v = 7 ORDER BY k LIMIT 5
SETTINGS query_plan_optimize_lazy_final = 0;

-- When all selected parts do not intersect by the primary key, the whole FINAL read is
-- replaced by a plain read. The replacement preserves the reading order and the early
-- exit, so it must stay enabled for read-in-order and small-limit queries.
-- Parts collapsed on insert get a non-zero level; unmerged level-0 parts are conservatively
-- treated as intersecting (they may contain duplicate keys inside), so pin the setting.
SET optimize_on_insert = 1;
DROP TABLE IF EXISTS t_lazy_final_gates_disjoint;
CREATE TABLE t_lazy_final_gates_disjoint (k UInt64, v UInt64, s String) ENGINE = ReplacingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_lazy_final_gates_disjoint;
INSERT INTO t_lazy_final_gates_disjoint SELECT number, number % 100, toString(number) FROM numbers(100000);
INSERT INTO t_lazy_final_gates_disjoint SELECT number + 100000, number % 100, toString(number + 100000) FROM numbers(100000);

SELECT 'disjoint read-in-order limit, final read:', countIf(explain LIKE '%FINAL: 1%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates_disjoint FINAL WHERE v = 7 ORDER BY k LIMIT 10);

SELECT 'disjoint small limit, final read:', countIf(explain LIKE '%FINAL: 1%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates_disjoint FINAL WHERE v = 7 LIMIT 10);

-- Control for the assertion above: with the optimization disabled the FINAL read stays.
SELECT 'disjoint lazy off, final read:', countIf(explain LIKE '%FINAL: 1%') > 0
FROM (EXPLAIN SELECT k FROM t_lazy_final_gates_disjoint FINAL WHERE v = 7 ORDER BY k LIMIT 10 SETTINGS query_plan_optimize_lazy_final = 0);

SELECT k, v FROM t_lazy_final_gates_disjoint FINAL WHERE v = 7 ORDER BY k LIMIT 5;

DROP TABLE t_lazy_final_gates;
DROP TABLE t_lazy_final_gates_disjoint;
