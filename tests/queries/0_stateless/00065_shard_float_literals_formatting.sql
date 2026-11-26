-- Tags: shard

SET optimize_skip_unused_shards = 0;
SELECT toTypeName(1.0) FROM remote('127.0.0.{2,3}', system, one)
