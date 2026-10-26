-- Lower an `arrayJoin` function into a real ARRAY JOIN step. Must never change results, so each case
-- compares query_plan_lower_array_join_function on vs off and expects equality (1).

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_laj;
CREATE TABLE t_laj (id UInt64, a Array(String), b Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_laj VALUES (1, ['x', 'y'], ['p', 'q']), (2, [], ['z']), (3, ['m'], []);

-- single arrayJoin
SELECT (SELECT groupArray(e) FROM (SELECT arrayJoin(a) AS e FROM t_laj ORDER BY e) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray(e) FROM (SELECT arrayJoin(a) AS e FROM t_laj ORDER BY e) SETTINGS query_plan_lower_array_join_function = 0);

-- two independent arrayJoins compose as a cross product
SELECT (SELECT count() FROM (SELECT arrayJoin(a) AS x, arrayJoin(b) AS y FROM t_laj) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT count() FROM (SELECT arrayJoin(a) AS x, arrayJoin(b) AS y FROM t_laj) SETTINGS query_plan_lower_array_join_function = 0);

-- empty arrays produce zero rows (inner semantics), preserved
SELECT (SELECT groupArray((id, x)) FROM (SELECT id, arrayJoin(a) AS x FROM t_laj ORDER BY id, x) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray((id, x)) FROM (SELECT id, arrayJoin(a) AS x FROM t_laj ORDER BY id, x) SETTINGS query_plan_lower_array_join_function = 0);

-- WHERE on the arrayJoined column
SELECT (SELECT groupArray(x) FROM (SELECT arrayJoin(a) AS x FROM t_laj WHERE x = 'x' ORDER BY x) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray(x) FROM (SELECT arrayJoin(a) AS x FROM t_laj WHERE x = 'x' ORDER BY x) SETTINGS query_plan_lower_array_join_function = 0);

-- with the setting on the function form becomes a real ArrayJoin step
SELECT countIf(explain LIKE '%ArrayJoin%') > 0 FROM (EXPLAIN SELECT arrayJoin(a) AS x FROM t_laj SETTINGS query_plan_lower_array_join_function = 1, serialize_query_plan = 0);

-- and the step 1 filter fusion then applies to the lowered step
SELECT countIf(explain LIKE '%Element filter%') > 0 FROM (EXPLAIN actions = 1 SELECT arrayJoin(a) AS x FROM t_laj WHERE x = 'x' SETTINGS query_plan_lower_array_join_function = 1, serialize_query_plan = 0);

-- #112241 headline: three independent arrayJoins, one per-array filter each. Each array is filtered before
-- its expansion - every element filter fuses into its own lowered step.
DROP TABLE IF EXISTS t_cart;
CREATE TABLE t_cart (A Array(String), B Array(String), C Array(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cart VALUES (['a', 'X-A'], ['b', 'X-B'], ['c', 'X-C']), (['a'], ['b'], ['c']);
SELECT (SELECT count() FROM (SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c FROM t_cart WHERE a = 'X-A' AND b = 'X-B' AND c = 'X-C') SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT count() FROM (SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c FROM t_cart WHERE a = 'X-A' AND b = 'X-B' AND c = 'X-C') SETTINGS query_plan_lower_array_join_function = 0);
SELECT countIf(explain LIKE '%Element filter%') = 3 FROM (EXPLAIN actions = 1 SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c FROM t_cart WHERE a = 'X-A' AND b = 'X-B' AND c = 'X-C' SETTINGS query_plan_lower_array_join_function = 1, serialize_query_plan = 0, query_plan_merge_filters = 1);
DROP TABLE t_cart;

-- the array argument is reused above the join: the lowered step must forward the already-computed column,
-- not demand a fresh one (regression: `Not found column arraySort(a)`).
DROP TABLE IF EXISTS t_reuse;
CREATE TABLE t_reuse (a Array(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_reuse VALUES ([3, 1, 2]), ([5]);
SELECT (SELECT groupArray((x, l)) FROM (SELECT arrayJoin(arraySort(a)) AS x, length(arraySort(a)) AS l FROM t_reuse ORDER BY x) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray((x, l)) FROM (SELECT arrayJoin(arraySort(a)) AS x, length(arraySort(a)) AS l FROM t_reuse ORDER BY x) SETTINGS query_plan_lower_array_join_function = 0);
DROP TABLE t_reuse;

-- a query-scope non-deterministic function keeps its pre-expansion value (computed once per source row and
-- replicated), not one value per expanded row. 2 source rows -> at most 2 distinct values.
SELECT (SELECT count(DISTINCT r) FROM (SELECT arrayJoin([1, 2, 3]) AS e, rand() AS r FROM numbers(2)) SETTINGS query_plan_lower_array_join_function = 1) <= 2;

-- a query-scope non-deterministic function in the WHERE would flip from row-replicated to per-expanded-row
-- if lowered, so the pass must decline (no ArrayJoin step appears).
SELECT countIf(explain LIKE '%ArrayJoin (ARRAY JOIN)%') = 0 FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS e FROM numbers(2) WHERE e > 0 AND rand() % 2 = 0 SETTINGS query_plan_lower_array_join_function = 1, serialize_query_plan = 0);
-- but a deterministic filter over the same arrayJoin still lowers
SELECT countIf(explain LIKE '%ArrayJoin (ARRAY JOIN)%') > 0 FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS e FROM numbers(2) WHERE e > 1 SETTINGS query_plan_lower_array_join_function = 1, serialize_query_plan = 0);
-- count() over an arrayJoin whose element is unused still returns the right number of rows
SELECT (SELECT count() FROM (SELECT arrayJoin([1, 2, 3]) FROM numbers(4)) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT count() FROM (SELECT arrayJoin([1, 2, 3]) FROM numbers(4)) SETTINGS query_plan_lower_array_join_function = 0);

-- the raw array input is reused above the join under a distinct name: the passenger stays an array
SELECT (SELECT groupArray((l, e)) FROM (SELECT length(a) AS l, arrayJoin(a) AS e FROM t_laj ORDER BY e) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray((l, e)) FROM (SELECT length(a) AS l, arrayJoin(a) AS e FROM t_laj ORDER BY e) SETTINGS query_plan_lower_array_join_function = 0);

-- independent arrayJoins keep their nesting: the first one written is the outer loop
SELECT (SELECT groupArray((a, b)) FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray((a, b)) FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b) SETTINGS query_plan_lower_array_join_function = 0);
SELECT (SELECT groupArray((a, b, c)) FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b, arrayJoin([100, 200]) AS c) SETTINGS query_plan_lower_array_join_function = 1)
     = (SELECT groupArray((a, b, c)) FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b, arrayJoin([100, 200]) AS c) SETTINGS query_plan_lower_array_join_function = 0);
-- and pin the order itself
SELECT groupArray((a, b)) FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b) SETTINGS query_plan_lower_array_join_function = 1;

DROP TABLE t_laj;
