SELECT groupArrayOrderBy(2)(x, y, z) FROM (SELECT number AS x, number % 3 AS y, number % 5 AS z FROM system.numbers LIMIT 10);
SELECT groupArrayOrderBy(1)(x, y) FROM (SELECT number AS x, number % 3 AS y FROM system.numbers_mt LIMIT 10);
SELECT groupArrayOrderBy(1)(x, y) FROM (SELECT number AS x, number AS y FROM system.numbers_mt LIMIT 10);
