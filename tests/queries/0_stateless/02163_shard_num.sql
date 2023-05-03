-- { echo }
SELECT shardNum() AS shard_num, sum(1) as rows FROM remote('127.{1,2}', system, one) GROUP BY _shard_num;
SELECT shardNum() AS shard_num, sum(1) as rows FROM remote('127.{1,2}', system, one) GROUP BY shard_num;
SELECT _shard_num AS shard_num, sum(1) as rows FROM remote('127.{1,2}', system, one) GROUP BY _shard_num;
SELECT _shard_num AS shard_num, sum(1) as rows FROM remote('127.{1,2}', system, one) GROUP BY shard_num;
SELECT a._shard_num AS shard_num, sum(1) as rows FROM remote('127.{1,2}', system, one) a GROUP BY shard_num;
SELECT _shard_num FROM remote('127.1', system.one) AS a INNER JOIN (SELECT _shard_num FROM system.one) AS b USING (dummy); -- { serverError UNKNOWN_IDENTIFIER }
