-- A lambda is not a column: the lift below ARRAY JOIN must not carry it through the step
-- (debug builds trip on cutting the Function-typed constant in the step).

SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM system.one ARRAY JOIN [[], ['a'], ['a', 'b'], []] AS x;
SELECT count() FROM (EXPLAIN header = 1 SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM system.one ARRAY JOIN [[], ['a'], ['a', 'b'], []] AS x) WHERE explain ILIKE '%Function(%';
SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x) SETTINGS query_plan_lower_array_join_function = 1;
SELECT count() FROM (EXPLAIN header = 1 SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x) SETTINGS query_plan_lower_array_join_function = 1) WHERE explain ILIKE '%Function(%';
-- a higher-order call that does not read the joined column still moves below the join, lambda included
SELECT arrayExists(i -> arr[idx + i] LIKE '%arrayMap(%', [1, 2]) FROM (SELECT groupArray(explain) AS arr, arrayFirstIndex(e -> e LIKE '%ArrayJoin (ARRAY JOIN)%', arr) AS idx FROM (EXPLAIN header = 1 SELECT x, arrayMap(y -> y + 1, arr2) AS m FROM (SELECT range(number) AS arr1, range(number + 2) AS arr2 FROM numbers(3)) ARRAY JOIN arr1 AS x SETTINGS serialize_query_plan = 0));
-- a plain expression is still lifted below the join
SELECT position(s, 'ArrayJoin') < position(s, 'plus(') FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS s FROM (EXPLAIN header = 1 SELECT x, number + 1 AS z FROM numbers(1) ARRAY JOIN [1, 2] AS x SETTINGS serialize_query_plan = 0));
