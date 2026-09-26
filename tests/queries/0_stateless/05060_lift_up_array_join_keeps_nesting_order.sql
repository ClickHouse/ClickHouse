-- an arrayJoin above an ARRAY JOIN must not be lifted below it: that swaps the nesting and reorders the rows
SELECT groupArray((x, y)) FROM (SELECT x, arrayJoin([10, 20]) AS y FROM numbers(1) ARRAY JOIN [1, 2] AS x);
SELECT groupArray((x, y)) FROM (SELECT x, arrayJoin([10, 20]) AS y FROM numbers(1) ARRAY JOIN [1, 2] AS x) SETTINGS query_plan_lift_up_array_join = 0;
-- a liftable sibling stays above the join too
SELECT groupArray((x, y, z)) FROM (SELECT x, arrayJoin([10, 20]) AS y, number + 1 AS z FROM numbers(1) ARRAY JOIN [1, 2] AS x);
-- the lift itself still fires in the mixed case: the sibling `z` is computed below the join (it shows up in the ArrayJoin header)
SELECT position(s, 'ArrayJoin') < position(s, 'plus(') FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS s FROM (EXPLAIN header = 1 SELECT x, arrayJoin([10, 20]) AS y, number + 1 AS z FROM numbers(1) ARRAY JOIN [1, 2] AS x SETTINGS serialize_query_plan = 0));
