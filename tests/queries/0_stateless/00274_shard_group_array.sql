-- Tags: shard

SELECT length(groupArray(number)), count() FROM (SELECT number FROM system.numbers_mt LIMIT 1000000);
SELECT groupArray(dummy), count() FROM remote('127.0.0.{2,3}', system.one);

SELECT length(groupArray(toString(number))), count() FROM (SELECT number FROM system.numbers LIMIT 100000);
SELECT groupArray(toString(dummy)), count() FROM remote('127.0.0.{2,3}', system.one);
