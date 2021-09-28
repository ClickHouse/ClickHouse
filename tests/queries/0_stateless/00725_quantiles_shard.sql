-- Tags: shard

SELECT quantiles(0.5, 0.9)(number) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT quantilesExact(0.5, 0.9)(number) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT quantilesTDigest(0.5, 0.9)(number) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT quantilesDeterministic(0.5, 0.9)(number, number) FROM remote('127.0.0.{1,2}', numbers(10));
