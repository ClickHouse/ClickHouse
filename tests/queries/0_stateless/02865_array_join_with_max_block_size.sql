-- { echoOn }
set max_block_size = 10, enable_unaligned_array_join = true;
SELECT n, count(1) from (SELECT groupArray(number % 10) AS x FROM (SELECT * FROM numbers(100000))) ARRAY JOIN x as n group by n;
SELECT n % 10, count(1) from (SELECT range(0, number) as x FROM numbers(1000)) LEFT ARRAY JOIN x as n group by n % 10;
SELECT (m+n) % 10, count(1) from (SELECT range(0, number+1) as x, range(0, number+2) as y FROM numbers(100)) ARRAY JOIN x as m, y as n group by (m+n) % 10;

set max_block_size = 1000, enable_unaligned_array_join = true;
SELECT n, count(1) from (SELECT groupArray(number % 10) AS x FROM (SELECT * FROM numbers(100000))) ARRAY JOIN x as n group by n;
SELECT n % 10, count(1) from (SELECT range(0, number) as x FROM numbers(1000)) LEFT ARRAY JOIN x as n group by n % 10;
SELECT (m+n) % 10, count(1) from (SELECT range(0, number+1) as x, range(0, number+2) as y FROM numbers(100)) ARRAY JOIN x as m, y as n group by (m+n) % 10;

set max_block_size = 100000, enable_unaligned_array_join = true;
SELECT n, count(1) from (SELECT groupArray(number % 10) AS x FROM (SELECT * FROM numbers(100000))) ARRAY JOIN x as n group by n;
SELECT n % 10, count(1) from (SELECT range(0, number) as x FROM numbers(1000)) LEFT ARRAY JOIN x as n group by n % 10;
SELECT (m+n) % 10, count(1) from (SELECT range(0, number+1) as x, range(0, number+2) as y FROM numbers(100)) ARRAY JOIN x as m, y as n group by (m+n) % 10;
-- { echoOff }
