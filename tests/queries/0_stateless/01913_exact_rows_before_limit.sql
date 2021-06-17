drop table if exists test_rows;
create table test_rows(f1 int,f2 int)  engine=MergeTree partition by f1 order by f2;
insert into test_rows select  0,arrayJoin(range(10000));
insert into test_rows select  1,arrayJoin(range(10000));
select 0 from test_rows limit 1 FORMAT JSONCompact settings exact_rows_before_limit = 0,output_format_write_statistics = 0;
select 0 from test_rows limit 1 FORMAT JSONCompact settings exact_rows_before_limit = 1, output_format_write_statistics = 0;
drop table if exists test_rows;
