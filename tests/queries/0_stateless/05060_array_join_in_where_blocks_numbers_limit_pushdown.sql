-- an arrayJoin referenced only from WHERE (through a WITH alias) still multiplies the rows, so the LIMIT must not be
-- pushed into numbers(): the source would emit only LIMIT rows before the expansion and the filter would drop them all
WITH arrayJoin([number]) AS x SELECT number FROM numbers(100) WHERE x >= 3 LIMIT 3;
WITH arrayJoin([number]) AS x SELECT number FROM numbers(100) WHERE x >= 3 LIMIT 3 SETTINGS query_plan_lower_array_join_function = 1;
WITH arrayJoin(if(number < 3, [], [number])) AS x SELECT number FROM numbers(100) WHERE x >= 0 LIMIT 3 SETTINGS query_plan_lower_array_join_function = 1;
