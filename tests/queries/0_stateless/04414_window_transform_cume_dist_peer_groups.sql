-- `cume_dist` caches the peer-group-end row number instead of rescanning per row. Exercise dense peer
-- groups that cross block boundaries (`max_block_size = 1`): partitioned, descending, and nullable
-- `NULLS FIRST` / `NULLS LAST` ties.

SELECT k, cume_dist() OVER (ORDER BY k) AS cd FROM (SELECT intDiv(number, 3) AS k FROM numbers(9)) ORDER BY k, cd SETTINGS max_block_size = 1;
SELECT p, k, cume_dist() OVER (PARTITION BY p ORDER BY k) AS cd FROM (SELECT number % 2 AS p, intDiv(number, 2) % 3 AS k FROM numbers(12)) ORDER BY p, k, cd SETTINGS max_block_size = 1;
SELECT k, cume_dist() OVER (ORDER BY k DESC) AS cd FROM (SELECT intDiv(number, 3) AS k FROM numbers(9)) ORDER BY k DESC, cd SETTINGS max_block_size = 1;
SELECT k, cume_dist() OVER (ORDER BY k NULLS FIRST) AS cd FROM (SELECT if(number % 3 = 0, NULL, toNullable(intDiv(number, 2))) AS k FROM numbers(12)) ORDER BY k NULLS FIRST, cd SETTINGS max_block_size = 1;
SELECT k, cume_dist() OVER (ORDER BY k NULLS LAST) AS cd FROM (SELECT if(number % 3 = 0, NULL, toNullable(intDiv(number, 2))) AS k FROM numbers(12)) ORDER BY k NULLS LAST, cd SETTINGS max_block_size = 1;
