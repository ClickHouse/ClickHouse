-- Tags: shard

SELECT * FROM (SELECT * WHERE dummy GLOBAL IN (SELECT 0));
SELECT * FROM (SELECT * WHERE dummy GLOBAL IN (SELECT toUInt8(number) FROM system.numbers LIMIT 10));
SELECT * FROM (SELECT * FROM (SELECT * FROM system.numbers LIMIT 20) WHERE number GLOBAL IN (SELECT number FROM system.numbers LIMIT 10));
SELECT * FROM (SELECT * FROM remote('127.0.0.{2,3,4}', system.one) WHERE dummy GLOBAL IN (SELECT * FROM remote('127.0.0.{2,3}', system.one)));
