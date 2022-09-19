-- Tags: no-parallel
drop table if exists test_rows_compact_part;
create table test_rows_compact_part(f1 int,f2 int)  engine=MergeTree partition by f1 order by f2 settings min_bytes_for_wide_part=10485760;
insert into test_rows_compact_part select  0,arrayJoin(range(10000)) ;
insert into test_rows_compact_part select  1,arrayJoin(range(10000));
select 0 from test_rows_compact_part limit 1 FORMAT JSONCompact settings exact_rows_before_limit = 0,output_format_write_statistics = 0;
select 0 from test_rows_compact_part limit 1 FORMAT JSONCompact settings exact_rows_before_limit = 1, output_format_write_statistics = 0;
drop table if exists test_rows_compact_part;


drop table if exists test_rows_wide_part;
create table test_rows_wide_part(f1 int,f2 int)  engine=MergeTree partition by f1 order by f2 settings min_bytes_for_wide_part=0;
insert into test_rows_wide_part select  0,arrayJoin(range(10000)) ;
insert into test_rows_wide_part select  1,arrayJoin(range(10000));
select 0 from test_rows_wide_part limit 1 FORMAT JSONCompact settings exact_rows_before_limit = 0,output_format_write_statistics = 0;
select 0 from test_rows_wide_part limit 1 FORMAT JSONCompact settings exact_rows_before_limit = 1, output_format_write_statistics = 0;
drop table if exists test_rows_compact_part;