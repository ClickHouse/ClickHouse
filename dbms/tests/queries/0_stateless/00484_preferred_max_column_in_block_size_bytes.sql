drop table if exists tab_00484;
create table tab_00484 (date Date, x UInt64, s FixedString(128)) engine = MergeTree(date, (date, x), 8192);
insert into tab_00484 select today(), number, toFixedString('', 128) from system.numbers limit 8192;

set preferred_block_size_bytes = 2000000;
set preferred_max_column_in_block_size_bytes = 0;
select max(blockSize()), min(blockSize()), any(ignore(*)) from tab_00484;
set preferred_max_column_in_block_size_bytes = 128;
select max(blockSize()), min(blockSize()), any(ignore(*)) from tab_00484;
set preferred_max_column_in_block_size_bytes = 256;
select max(blockSize()), min(blockSize()), any(ignore(*)) from tab_00484;
set preferred_max_column_in_block_size_bytes = 2097152;
select max(blockSize()), min(blockSize()), any(ignore(*)) from tab_00484;
set preferred_max_column_in_block_size_bytes = 4194304;
select max(blockSize()), min(blockSize()), any(ignore(*)) from tab_00484;

drop table if exists tab_00484;
create table tab_00484 (date Date, x UInt64, s FixedString(128)) engine = MergeTree(date, (date, x), 32);
insert into tab_00484 select today(), number, toFixedString('', 128) from system.numbers limit 47;
set preferred_max_column_in_block_size_bytes = 1152;
select blockSize(), * from tab_00484 where x = 1 or x > 36 format Null;

drop table if exists tab_00484;
create table tab_00484 (date Date, x UInt64, s FixedString(128)) engine = MergeTree(date, (date, x), 8192);
insert into tab_00484 select today(), number, toFixedString('', 128) from system.numbers limit 10;
set preferred_max_column_in_block_size_bytes = 128;
select s from tab_00484 where s == '' format Null;

drop table if exists tab_00484;
create table tab_00484 (date Date, x UInt64, s String) engine = MergeTree(date, (date, x), 8192);
insert into tab_00484 select today(), number, 'abc' from system.numbers limit 81920;
set preferred_block_size_bytes = 0;
select count(*) from tab_00484 prewhere s != 'abc' format Null;
select count(*) from tab_00484 prewhere s = 'abc' format Null;
