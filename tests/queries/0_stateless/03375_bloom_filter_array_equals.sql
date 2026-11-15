create table test (x Array(String), index idx1 x type bloom_filter(0.025)) engine=MergeTree order by tuple();
insert into test values (['s1']);
select * from test where x = ['s1'];

