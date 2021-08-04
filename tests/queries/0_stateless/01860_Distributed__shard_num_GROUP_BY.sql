-- GROUP BY _shard_num
SELECT _shard_num, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num ORDER BY _shard_num;
SELECT _shard_num s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num ORDER BY _shard_num;

SELECT _shard_num + 1, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num + 1 ORDER BY _shard_num + 1;
SELECT _shard_num + 1 s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num + 1 ORDER BY _shard_num + 1;

SELECT _shard_num + dummy, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num + dummy ORDER BY _shard_num + dummy;
SELECT _shard_num + dummy s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num + dummy ORDER BY _shard_num + dummy;

SELECT _shard_num FROM remote('127.0.0.{1,2}', system.one) ORDER BY _shard_num;
SELECT _shard_num s FROM remote('127.0.0.{1,2}', system.one) ORDER BY _shard_num;

SELECT _shard_num s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY s order by s;

select materialize(_shard_num), * from remote('127.{1,2}', system.one) limit 1 by dummy format Null;
