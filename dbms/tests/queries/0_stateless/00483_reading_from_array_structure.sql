drop table if exists table;
create table `table` (date Date, `Struct.Key1` Array(UInt64), `Struct.Key2` Array(UInt64), padding FixedString(16)) engine = MergeTree(date, (date), 16);
insert into `table` select today() as date, [number], [number + 1], toFixedString('', 16) from system.numbers limit 100;
set preferred_max_column_in_block_size_bytes = 96;
select blockSize(), * from `table` prewhere `Struct.Key1`[1] = 19 and `Struct.Key2`[1] >= 0 format Null;

drop table if exists `table`;
create table `table` (date Date, `Struct.Key1` Array(UInt64), `Struct.Key2` Array(UInt64), padding FixedString(16), x UInt64) engine = MergeTree(date, (date), 8);
insert into `table` select today() as date, [number], [number + 1], toFixedString('', 16), number from system.numbers limit 100;
set preferred_max_column_in_block_size_bytes = 112;
select blockSize(), * from table prewhere x = 7 format Null;

drop table if exists `table`;
