SET compile = 1, min_count_to_compile = 0, max_threads = 1;
SELECT arrayJoin([1, 2, 1]) AS UserID, argMax('Hello', today()) AS res GROUP BY UserID;
