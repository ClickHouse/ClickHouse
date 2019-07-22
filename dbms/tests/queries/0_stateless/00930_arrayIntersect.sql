drop table if exists array_intersect;

create table array_intersect (date Date, arr Array(UInt8)) engine=MergeTree partition by date order by date;

insert into array_intersect values ('2019-01-01', [1,2,3]);
insert into array_intersect values ('2019-01-01', [1,2]);
insert into array_intersect values ('2019-01-01', [1]);
insert into array_intersect values ('2019-01-01', []);

select arrayIntersect(arr, [1,2]) from array_intersect order by arr;
select arrayIntersect(arr, []) from array_intersect order by arr;
select arrayIntersect([], arr) from array_intersect order by arr;
select arrayIntersect([1,2], arr) from array_intersect order by arr;
select arrayIntersect([1,2], [1,2,3,4]) from array_intersect order by arr;
select arrayIntersect([], []) from array_intersect order by arr;

optimize table array_intersect;

select arrayIntersect(arr, [1,2]) from array_intersect order by arr;
select arrayIntersect(arr, []) from array_intersect order by arr;
select arrayIntersect([], arr) from array_intersect order by arr;
select arrayIntersect([1,2], arr) from array_intersect order by arr;
select arrayIntersect([1,2], [1,2,3,4]) from array_intersect order by arr;
select arrayIntersect([], []) from array_intersect order by arr;

drop table if exists array_intersect;

