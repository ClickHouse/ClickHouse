SELECT quantilesTiming(0.5, 0.9)(number) FROM (SELECT number FROM system.numbers LIMIT 100);
SELECT quantilesTiming(0.9, 0.5)(number) FROM (SELECT number FROM system.numbers LIMIT 100);
SELECT quantilesTiming(0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99)(number) FROM (SELECT number FROM system.numbers LIMIT 100);
SELECT quantilesTiming(0.99, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.01)(number) FROM (SELECT number FROM system.numbers LIMIT 100);
