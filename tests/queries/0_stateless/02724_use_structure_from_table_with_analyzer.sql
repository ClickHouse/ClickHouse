-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

set allow_experimental_analyzer=1;
set use_structure_from_insertion_table_in_table_functions=2;
drop table if exists test;
create table test (c1 UInt32) engine=Memory;

insert into test select c1 from s3('http://localhost:11111/test/a.tsv');
select * from test order by c1;

drop table test;

