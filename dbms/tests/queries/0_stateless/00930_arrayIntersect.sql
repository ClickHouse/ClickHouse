drop table if exists test.array_intersect;

create table test.array_intersect (date Date, arr Array(UInt8)) engine=MergeTree partition by date order by date;

insert into test.array_intersect values ('2019-01-01', [1,2,3]);
insert into test.array_intersect values ('2019-01-01', [1,2]);
insert into test.array_intersect values ('2019-01-01', [1]);
insert into test.array_intersect values ('2019-01-01', []);

select arrayIntersect(arr, [1,2]) from test.array_intersect;
select arrayIntersect(arr, []) from test.array_intersect;
select arrayIntersect([], arr) from test.array_intersect;
select arrayIntersect([1,2], arr) from test.array_intersect;
select arrayIntersect([1,2], [1,2,3,4]) from test.array_intersect;
select arrayIntersect([], []) from test.array_intersect;

optimize table test.array_intersect;

select arrayIntersect(arr, [1,2]) from test.array_intersect;
select arrayIntersect(arr, []) from test.array_intersect;
select arrayIntersect([], arr) from test.array_intersect;
select arrayIntersect([1,2], arr) from test.array_intersect;
select arrayIntersect([1,2], [1,2,3,4]) from test.array_intersect;
select arrayIntersect([], []) from test.array_intersect;

drop table if exists test.array_intersect;

