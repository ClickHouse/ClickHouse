SELECT range(x) AS k, count() FROM (SELECT number % 2 ? number : 0 AS x FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
