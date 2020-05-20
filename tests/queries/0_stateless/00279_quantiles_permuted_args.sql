SELECT quantilesExact(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesExactWeighted(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x, 1) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesTiming(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
-- SELECT quantilesTDigest(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
-- SELECT quantilesTDigestWeighted(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x, 1) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantilesDeterministic(1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4)(x, x) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
