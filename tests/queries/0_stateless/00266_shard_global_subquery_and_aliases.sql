-- Tags: shard

SELECT 1 GLOBAL IN (SELECT 1) AS s, s FROM remote('127.0.0.{2,3}', system.one);
