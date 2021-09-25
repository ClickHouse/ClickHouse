drop table if exists tab;
create table tab (i8 Int8, i16 Int16, i32 Int32, i64 Int64, i128 Int128, i256 Int256, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, u128 UInt128, u256 UInt256, id UUID, s String, fs FixedString(33), a Array(UInt8), t Tuple(UInt16, UInt32), d Date, dt DateTime('Europe/Moscow'), dt64 DateTime64(3, 'Europe/Moscow'), dec128 Decimal128(3), dec256 Decimal256(4), lc LowCardinality(String)) engine = MergeTree PARTITION BY (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, id, s, fs, a, t, d, dt, dt64, dec128, dec256, lc) order by tuple();
insert into tab values (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'a', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', [1, 2, 3], (-1, -2), '2020-01-01', '2020-01-01 01:01:01', '2020-01-01 01:01:01', '123.456', '78.9101', 'a');
-- Here we check that partition id did not change.
-- Different result means Backward Incompatible Change. Old partitions will not be accepted by new server.
select partition_id from system.parts where table = 'tab' and database = currentDatabase();
drop table if exists tab;
