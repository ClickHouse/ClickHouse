SELECT * FROM (SELECT range(number) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> toNullable(x), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, x), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, x + 1), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, toNullable(x)), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, nullIf(x, 3)), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
