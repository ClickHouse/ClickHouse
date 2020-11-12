-- GROUP BY _shard_num
SELECT _shard_num, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num;
SELECT _shard_num s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY _shard_num;
