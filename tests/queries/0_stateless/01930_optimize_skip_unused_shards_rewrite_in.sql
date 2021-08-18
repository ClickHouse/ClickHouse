set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;

create temporary table data (id UInt64) engine=Memory() as with [
    0,
    1,
    0x7f, 0x80, 0xff,
    0x7fff, 0x8000, 0xffff,
    0x7fffffff, 0x80000000, 0xffffffff,
    0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff
] as values select arrayJoin(values) id;

-- { echoOn }

-- Int8, Int8
select _shard_num, * from remote('127.{1..4}', view(select toInt8(id) id from data), toInt8(id)) where id in (0, 1, 0x7f) order by _shard_num, id;
-- Int8, UInt8
select _shard_num, * from remote('127.{1..4}', view(select toInt8(id) id from data), toUInt8(id)) where id in (0, 1, 0x7f) order by _shard_num, id;
-- UInt8, UInt8
select _shard_num, * from remote('127.{1..4}', view(select toUInt8(id) id from data), toUInt8(id)) where id in (0, 1, 0x7f, 0x80, 0xff) order by _shard_num, id;
-- UInt8, Int8
select _shard_num, * from remote('127.{1..4}', view(select toUInt8(id) id from data), toInt8(id)) where id in (0, 1, 0x7f, 0x80, 0xff) order by _shard_num, id;

-- Int16, Int16
select _shard_num, * from remote('127.{1..4}', view(select toInt16(id) id from data), toInt16(id)) where id in (0, 1, 0x7fff) order by _shard_num, id;
-- Int16, UInt16
select _shard_num, * from remote('127.{1..4}', view(select toInt16(id) id from data), toUInt16(id)) where id in (0, 1, 0x7fff) order by _shard_num, id;
-- UInt16, UInt16
select _shard_num, * from remote('127.{1..4}', view(select toUInt16(id) id from data), toUInt16(id)) where id in (0, 1, 0x7fff, 0x8000, 0xffff) order by _shard_num, id;
-- UInt16, Int16
select _shard_num, * from remote('127.{1..4}', view(select toUInt16(id) id from data), toInt16(id)) where id in (0, 1, 0x7fff, 0x8000, 0xffff) order by _shard_num, id;

-- Int32, Int32
select _shard_num, * from remote('127.{1..4}', view(select toInt32(id) id from data), toInt32(id)) where id in (0, 1, 0x7fffffff) order by _shard_num, id;
-- Int32, UInt32
select _shard_num, * from remote('127.{1..4}', view(select toInt32(id) id from data), toUInt32(id)) where id in (0, 1, 0x7fffffff) order by _shard_num, id;
-- UInt32, UInt32
select _shard_num, * from remote('127.{1..4}', view(select toUInt32(id) id from data), toUInt32(id)) where id in (0, 1, 0x7fffffff, 0x80000000, 0xffffffff) order by _shard_num, id;
-- UInt32, Int32
select _shard_num, * from remote('127.{1..4}', view(select toUInt32(id) id from data), toInt32(id)) where id in (0, 1, 0x7fffffff, 0x80000000, 0xffffffff) order by _shard_num, id;

-- Int64, Int64
select _shard_num, * from remote('127.{1..4}', view(select toInt64(id) id from data), toInt64(id)) where id in (0, 1, 0x7fffffffffffffff) order by _shard_num, id;
-- Int64, UInt64
select _shard_num, * from remote('127.{1..4}', view(select toInt64(id) id from data), toUInt64(id)) where id in (0, 1, 0x7fffffffffffffff) order by _shard_num, id;
-- UInt64, UInt64
select _shard_num, * from remote('127.{1..4}', view(select toUInt64(id) id from data), toUInt64(id)) where id in (0, 1, 0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff) order by _shard_num, id;
-- UInt64, Int64
select _shard_num, * from remote('127.{1..4}', view(select toUInt64(id) id from data), toInt64(id)) where id in (0, 1, 0x7fffffffffffffff, 0x8000000000000000, 0xffffffffffffffff) order by _shard_num, id;

-- modulo(Int8)
select distinct _shard_num, * from remote('127.{1..4}', view(select toInt16(id) id from data), toInt8(id)%255) where id in (-1) order by _shard_num, id;
-- modulo(UInt8)
select distinct _shard_num, * from remote('127.{1..4}', view(select toInt16(id) id from data), toUInt8(id)%255) where id in (-1) order by _shard_num, id;

-- { echoOff }

-- those two had been reported initially by amosbird:
-- (the problem is that murmurHash3_32() returns different value to toInt64(1) and toUInt64(1))
---- error for local node
select * from remote('127.{1..4}', view(select number id from numbers(0)), bitAnd(murmurHash3_32(id), 2147483647)) where id in (2, 3);
---- error for remote node
select * from remote('127.{1..8}', view(select number id from numbers(0)), bitAnd(murmurHash3_32(id), 2147483647)) where id in (2, 3);
