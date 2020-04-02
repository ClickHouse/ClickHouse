DROP TABLE IF EXISTS d_one;
CREATE TABLE d_one (dummy UInt8) ENGINE = Distributed(test_cluster_two_shards, system, one, rand());

SELECT 'local_0', toUInt8(1) AS dummy FROM system.one AS o WHERE o.dummy = 0;
SELECT 'local_1', toUInt8(1) AS dummy FROM system.one AS o WHERE o.dummy = 1;

SELECT 'distributed_0', _shard_num, toUInt8(1) AS dummy FROM d_one AS o WHERE o.dummy = 0 ORDER BY _shard_num;
SELECT 'distributed_1', _shard_num, toUInt8(1) AS dummy FROM d_one AS o WHERE o.dummy = 1 ORDER BY _shard_num;

SET distributed_product_mode = 'local';

SELECT 'local_0', toUInt8(1) AS dummy FROM system.one AS o WHERE o.dummy = 0;
SELECT 'local_1', toUInt8(1) AS dummy FROM system.one AS o WHERE o.dummy = 1;

SELECT 'distributed_0', _shard_num, toUInt8(1) AS dummy FROM d_one AS o WHERE o.dummy = 0 ORDER BY _shard_num;
SELECT 'distributed_1', _shard_num, toUInt8(1) AS dummy FROM d_one AS o WHERE o.dummy = 1 ORDER BY _shard_num;

DROP TABLE d_one;

SELECT 'remote_0', toUInt8(1) AS dummy FROM remote('127.0.0.2', system, one) AS o WHERE o.dummy = 0;
SELECT 'remote_1', toUInt8(1) AS dummy FROM remote('127.0.0.2', system, one) AS o WHERE o.dummy = 1;
