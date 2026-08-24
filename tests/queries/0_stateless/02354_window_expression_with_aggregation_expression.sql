SELECT 
    sum(a)*100/sum(sum(a)) OVER (PARTITION BY b) AS r
FROM 
(
  SELECT 1 AS a, 2 AS b
  UNION ALL
  SELECT 3 AS a, 4 AS b
  UNION ALL
  SELECT 5 AS a, 2 AS b

) AS t
GROUP BY b;

-- { echoOn }
SELECT arrayMap(x -> (x + 1), groupArray(number) OVER ()) AS result
FROM numbers(10);
