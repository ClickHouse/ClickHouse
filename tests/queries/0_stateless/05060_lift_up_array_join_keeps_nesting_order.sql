-- an arrayJoin above an ARRAY JOIN must not be lifted below it: that swaps the nesting and reorders the rows
SELECT groupArray((x, y)) FROM (SELECT x, arrayJoin([10, 20]) AS y FROM numbers(1) ARRAY JOIN [1, 2] AS x);
SELECT groupArray((x, y)) FROM (SELECT x, arrayJoin([10, 20]) AS y FROM numbers(1) ARRAY JOIN [1, 2] AS x) SETTINGS query_plan_lift_up_array_join = 0;
-- a liftable sibling stays above the join too
SELECT groupArray((x, y, z)) FROM (SELECT x, arrayJoin([10, 20]) AS y, number + 1 AS z FROM numbers(1) ARRAY JOIN [1, 2] AS x);
