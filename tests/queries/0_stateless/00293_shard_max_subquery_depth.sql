-- Tags: shard

SET max_subquery_depth = 3;

SELECT 1 FROM remote('127.0.0.{1,2}', system.one) WHERE 1 GLOBAL IN (SELECT 1 FROM remote('127.0.0.{2,3}', system.one) WHERE 1 GLOBAL IN (SELECT 1 FROM remote('127.0.0.{2,3}', system.one) WHERE 1 GLOBAL IN (SELECT 1 FROM remote('127.0.0.{2,3}', system.one))));
SELECT 2 FROM system.one WHERE 1 IN (SELECT 1 FROM system.one WHERE 1 IN (SELECT 1 FROM system.one WHERE 1 IN (SELECT 1 FROM system.one)));
