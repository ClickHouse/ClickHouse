SELECT number * 246 + 10 AS n, toDate('2000-01-01') + n AS d, range(n) AS arr, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT LineAsString;
