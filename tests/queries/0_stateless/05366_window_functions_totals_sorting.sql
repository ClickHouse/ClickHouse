-- The query from the Stress test failure (Nullable key with NULL and NaN).
SELECT k, rank() OVER (PARTITION BY CAST(arrayJoin([NULL, -1, 0, 1, 2, nan, NULL]), 'Nullable(Float32)'), k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH TOTALS ORDER BY ALL ASC NULLS LAST;

-- Q2: partition starts on the totals rows.
SELECT arrayJoin([2, 1, 2]) AS x, count() OVER (PARTITION BY x) AS c
FROM values('k String', 'a') GROUP BY k WITH TOTALS ORDER BY x;

-- Q3: peer groups on the totals rows.
SELECT arrayJoin([1, 2, 1]) AS x, rank() OVER (ORDER BY x DESC) AS r
FROM values('k String', 'a') GROUP BY k WITH TOTALS ORDER BY x DESC;

-- Q4: totals split into several chunks.
SELECT y, count() OVER (PARTITION BY y) AS c
FROM (SELECT arrayJoin([[2, 1, 2], [2, 1, 2], [2, 1, 2]]) AS a FROM values('k String', 'a') GROUP BY k WITH TOTALS)
ARRAY JOIN a AS y
ORDER BY y
SETTINGS max_block_size = 1;

-- Q5: the sort limits apply to the totals rows too.
SELECT arrayJoin(if(k = '', range(10), [0])) AS x, count() OVER (PARTITION BY x) AS c
FROM values('k String', 'a') GROUP BY k WITH TOTALS
FORMAT Null SETTINGS max_rows_to_sort = 5; -- { serverError TOO_MANY_ROWS_OR_BYTES }
