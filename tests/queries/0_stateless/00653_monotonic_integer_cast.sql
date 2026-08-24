drop table if exists `table_00653`;
create table `table_00653` (val Int32) engine = MergeTree order by val;
insert into `table_00653` values (-2), (0), (2);
select count() from `table_00653` where toUInt64(val) == 0;
drop table table_00653;
