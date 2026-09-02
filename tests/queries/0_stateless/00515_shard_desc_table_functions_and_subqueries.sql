-- Tags: shard

drop table if exists tab;
set allow_deprecated_syntax_for_merge_tree=1;
create table tab (date Date, val UInt64, val2 UInt8 default 42, val3 UInt8 default val2 + 1, val4 UInt64 alias val) engine = MergeTree(date, (date, val), 8192);
desc tab;
select '-';
desc table tab;
select '-';
desc remote('127.0.0.2', currentDatabase(), tab);
select '-';
desc table remote('127.0.0.2', currentDatabase(), tab);
select '-';
desc (select 1);
select '-';
desc table (select 1);
select '-';
desc (select * from system.numbers);
select '-';
drop table if exists tab;
