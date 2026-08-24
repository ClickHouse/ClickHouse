-- Tags: shard

SELECT number FROM remote('127.0.0.{2,3}', system, numbers) WHERE number GLOBAL IN (SELECT number FROM remote('127.0.0.{2,3}', system, numbers) WHERE number % 2 = 1 LIMIT 10) LIMIT 10;
