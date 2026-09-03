-- liftUpArrayJoin must not move an arrayJoin function below an ARRAY JOIN clause: the two expansions would swap
-- nesting and the cross product would come out in a different row order (the clause join is the outer loop).
SELECT groupArray((x, y)) FROM (SELECT x, arrayJoin([10, 20]) AS y FROM numbers(1) ARRAY JOIN [1, 2] AS x);
SELECT groupArray((x, y)) FROM (SELECT x, arrayJoin([10, 20]) AS y FROM numbers(1) ARRAY JOIN [1, 2] AS x) SETTINGS query_plan_lift_up_array_join = 0;
-- a liftable sibling in the same expression just stays above the join too; the result is unchanged
SELECT groupArray((x, y, z)) FROM (SELECT x, arrayJoin([10, 20]) AS y, number + 1 AS z FROM numbers(1) ARRAY JOIN [1, 2] AS x);
