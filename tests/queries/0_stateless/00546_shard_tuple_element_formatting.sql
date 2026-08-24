-- Tags: shard

SELECT tupleElement((1, 2), toUInt8(1 + 0)) FROM remote('127.0.0.{2,3}', system.one);
