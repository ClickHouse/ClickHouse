drop table if exists test.table;
create table test.table (val Int32) engine = MergeTree order by val;
insert into test.table values (-2), (0), (2);
select count() from test.table where toUInt64(val) == 0;

