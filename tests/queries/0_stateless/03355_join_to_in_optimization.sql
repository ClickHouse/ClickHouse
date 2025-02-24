
explain actions=1, optimize=1, header=1 SELECT a.number
FROM numbers_mt(1000000000) AS a,
     (SELECT number * 13 AS x
     FROM numbers_mt(1000)) AS b
WHERE a.number = b.x
