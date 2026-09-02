-- Tags: memory-engine
create table test_memory (number UInt64) engine=Memory;
insert into test_memory select 42;
drop table test_memory settings ignore_drop_queries_probability=1;
select * from test_memory;
drop table test_memory;

create table test_merge_tree (number UInt64) engine=MergeTree order by number;
insert into test_merge_tree select 42;
drop table test_merge_tree settings ignore_drop_queries_probability=1;
select * from test_merge_tree;
drop table test_merge_tree;

create table test_join (number UInt64) engine=Join(ALL, LEFT, number);
insert into test_join select 42;
drop table test_join settings ignore_drop_queries_probability=1;
select * from test_join;
drop table test_join;

