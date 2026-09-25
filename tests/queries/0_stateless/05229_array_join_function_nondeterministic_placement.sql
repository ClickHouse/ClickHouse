-- With arrayJoin lowered to a step, a non-deterministic function that does not depend on the joined value is drawn once
-- per output row, like with the ARRAY JOIN clause. The setting restores the old draw once per source row.
SET query_plan_lower_array_join_function = 1;

SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, rand64() AS r FROM numbers(2));
SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, rand64() AS r FROM numbers(2)) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;
SELECT uniqExact(id), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, generateUUIDv4() AS id FROM numbers(2));
SELECT uniqExact(id), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, generateUUIDv4() AS id FROM numbers(2)) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;

-- applied to a column that crosses the expansion
SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, rand64(number) AS r FROM numbers(2));
SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, rand64(number) AS r FROM numbers(2)) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;

-- applied to the element: per output row in both modes
SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2, 3]) AS e, rand64(e) AS r FROM numbers(2)) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;

-- inside the array argument: per source row in both modes, and the same rand() node above the join sees the same draw
SELECT count() FROM (SELECT arrayJoin(range(rand() % 3 + 1)) AS e, rand() % 3 + 1 AS n FROM numbers(100)) WHERE e >= n;
SELECT count() FROM (SELECT arrayJoin(range(rand() % 3 + 1)) AS e, rand() % 3 + 1 AS n FROM numbers(100)) WHERE e >= n SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;

-- a filter computed next to the arrayJoin: per output row keeps some of a source row's expanded rows, per source row keeps all or none
SELECT countIf(c NOT IN (0, 3)) > 0 FROM (SELECT number, count() AS c FROM (SELECT number, arrayJoin([1, 2, 3]) AS e, rand() % 2 = 0 AS keep FROM numbers(1000)) WHERE keep GROUP BY number);
SELECT countIf(c NOT IN (0, 3)) > 0 FROM (SELECT number, count() AS c FROM (SELECT number, arrayJoin([1, 2, 3]) AS e, rand() % 2 = 0 AS keep FROM numbers(1000)) WHERE keep GROUP BY number) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;

-- both modes lower the join
SELECT countIf(explain LIKE '%ArrayJoin (ARRAY JOIN)%') FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS e, rand64() AS r FROM numbers(2) SETTINGS serialize_query_plan = 0);
SELECT countIf(explain LIKE '%ArrayJoin (ARRAY JOIN)%') FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS e, rand64() AS r FROM numbers(2) SETTINGS serialize_query_plan = 0, arrayjoin_nondeterministic_functions_before_expansion = 1);

-- a stateful function keeps the function form: one block for the whole expansion, computed before it when independent
SELECT groupArray(rn) FROM (SELECT arrayJoin(range(3)) AS x, rowNumberInBlock() AS rn FROM numbers(2));
SELECT uniqExact(d), max(d) FROM (SELECT runningDifference(arrayJoin(range(50000))) AS d FROM numbers(2)) SETTINGS allow_deprecated_error_prone_window_functions = 1, max_block_size = 65505, max_threads = 1;
SELECT countIf(explain LIKE '%ArrayJoin (ARRAY JOIN)%') FROM (EXPLAIN SELECT arrayJoin([1, 2, 3]) AS e, rowNumberInBlock() AS rn FROM numbers(2) SETTINGS serialize_query_plan = 0);

-- two joins: a draw under the second join stays behind both, the order of the joins does not change
SELECT groupArray((a, b)), uniqExact(r), count() FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b, rand64(b) AS r) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;
SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b, rand64(a) AS r) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;
SELECT uniqExact(r), count() FROM (SELECT arrayJoin([1, 2]) AS a, arrayJoin([10, 20]) AS b, rand64() AS r) SETTINGS arrayjoin_nondeterministic_functions_before_expansion = 1;

