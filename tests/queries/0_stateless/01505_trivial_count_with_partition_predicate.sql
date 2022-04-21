drop table if exists test1;
drop table if exists test_tuple;
drop table if exists test_two_args;

create table test1(p DateTime, k int) engine MergeTree partition by toDate(p) order by k settings index_granularity = 1;
insert into test1 values ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);

set max_rows_to_read = 1;
-- non-optimized
select count() from test1 settings max_parallel_replicas = 3; -- { serverError 158; }
-- optimized (toYear is monotonic and we provide the partition expr as is)
select count() from test1 where toYear(toDate(p)) = 1999;
-- non-optimized (toDate(DateTime) is always monotonic, but we cannot relaxing the predicates to do trivial count())
select count() from test1 where p > toDateTime('2020-09-01 10:00:00'); -- { serverError 158; }
-- optimized (partition expr wrapped with non-monotonic functions)
select count() FROM test1 where toDate(p) = '2020-09-01' and sipHash64(toString(toDate(p))) % 2 = 1;
select count() FROM test1 where toDate(p) = '2020-09-01' and sipHash64(toString(toDate(p))) % 2 = 0;
-- non-optimized (some predicate depends on non-partition_expr columns)
select count() FROM test1 where toDate(p) = '2020-09-01' and k = 2; -- { serverError 158; }
-- optimized
select count() from test1 where toDate(p) > '2020-09-01';
-- non-optimized
select count() from test1 where toDate(p) >= '2020-09-01' and p <= '2020-09-01 00:00:00';

create table test_tuple(p DateTime, i int, j int) engine MergeTree partition by (toDate(p), i) order by j settings index_granularity = 1;

insert into test_tuple values ('2020-09-01 00:01:02', 1, 2), ('2020-09-01 00:01:03', 2, 3), ('2020-09-02 00:01:03', 3, 4);

-- optimized
select count() from test_tuple where toDate(p) > '2020-09-01';
-- optimized
select count() from test_tuple where toDate(p) > '2020-09-01' and i = 1;
-- optimized
select count() from test_tuple where i > 2;
-- optimized
select count() from test_tuple where i < 1;
-- non-optimized
select count() from test_tuple array join [p,p] as c where toDate(p) = '2020-09-01'; -- { serverError 158; }
select count() from test_tuple array join [1,2] as c where toDate(p) = '2020-09-01' settings max_rows_to_read = 4;
-- non-optimized
select count() from test_tuple array join [1,2,3] as c where toDate(p) = '2020-09-01'; -- { serverError 158; }
select count() from test_tuple array join [1,2,3] as c where toDate(p) = '2020-09-01' settings max_rows_to_read = 6;

create table test_two_args(i int, j int, k int) engine MergeTree partition by i + j order by k settings index_granularity = 1;

insert into test_two_args values (1, 2, 3), (2, 1, 3), (0, 3, 4);

-- optimized
select count() from test_two_args where i + j = 3;
-- non-optimized
select count() from test_two_args where i = 1; -- { serverError 158; }

drop table test1;
drop table test_tuple;
drop table test_two_args;
