drop table if exists test.tab;
create table test.tab (date Date, val UInt64, val2 UInt8 default 42, val3 UInt8 default val2 + 1, val4 UInt64 alias val) engine = MergeTree(date, (date, val), 8192);
desc test.tab;
select '-';
desc table test.tab;
select '-';
desc remote('localhost', test.tab);
select '-';
desc table remote('localhost', test.tab);
select '-';
desc (select 1);
select '-';
desc table (select 1);
select '-';
desc (select * from system.numbers);
select '-';

