-- A lambda is not a column: the lift below ARRAY JOIN must not carry it through the step
-- (debug builds trip on cutting the Function-typed constant in the step).

SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM system.one ARRAY JOIN [[], ['a'], ['a', 'b'], []] AS x;
SELECT count() FROM (EXPLAIN header = 1 SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM system.one ARRAY JOIN [[], ['a'], ['a', 'b'], []] AS x) WHERE explain ILIKE '%Function(%';
SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x) SETTINGS query_plan_lower_array_join_function = 1;
SELECT count() FROM (EXPLAIN header = 1 SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x) SETTINGS query_plan_lower_array_join_function = 1) WHERE explain ILIKE '%Function(%';
-- a plain expression is still lifted below the join
SELECT position(s, 'ArrayJoin') < position(s, 'plus(') FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS s FROM (EXPLAIN header = 1 SELECT x, number + 1 AS z FROM numbers(1) ARRAY JOIN [1, 2] AS x SETTINGS serialize_query_plan = 0));
