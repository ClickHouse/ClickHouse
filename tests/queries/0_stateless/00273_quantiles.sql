SELECT quantiles(0.5)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesExact(0.5)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesTDigest(0.5)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesDeterministic(0.5)(x, x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);

SELECT quantiles(0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesExact(0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesTDigest(0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesDeterministic(0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1)(x, x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);

SELECT round(1000000 / (number + 1)) AS k, count() AS c, quantilesDeterministic(0.1, 0.5, 0.9)(number, intHash64(number)) AS q1, quantilesExact(0.1, 0.5, 0.9)(number) AS q2 FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY k ORDER BY k;
