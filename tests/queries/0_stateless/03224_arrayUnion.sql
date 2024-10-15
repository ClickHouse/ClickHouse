drop table if exists array_union;

create table array_union (date Date, arr Array(UInt8)) engine=MergeTree partition by date order by date;

insert into array_union values ('2019-01-01', [1,2,3]);
insert into array_union values ('2019-01-01', [1,2]);
insert into array_union values ('2019-01-01', [1]);
insert into array_union values ('2019-01-01', []);


select arraySort(arrayUnion(arr, [1,2])) from array_union order by arr;
select '-------';
select arraySort(arrayUnion(arr, [])) from array_union order by arr;
select '-------';
select arraySort(arrayUnion([], arr)) from array_union order by arr;
select '-------';
select arraySort(arrayUnion([1,2], arr)) from array_union order by arr;
select '-------';
select arraySort(arrayUnion([1,2], [1,2,3,4])) from array_union order by arr;
select '-------';
select arraySort(arrayUnion([], [])) from array_union order by arr;

drop table if exists array_union;

select '-------';
select arraySort(arrayUnion([-100], [156]));
select '-------';
select arraySort(arrayUnion([1], [-257, -100]));
select '-------';
select arraySort(arrayUnion(['hi'], ['hello', 'hi'], []));
select '-------';
SELECT arraySort(arrayUnion([1, 2, NULL], [1, 3, NULL], [2, 3, NULL]));
select '-------';
SELECT arraySort(arrayUnion([NULL, NULL, NULL, 1], [1, NULL, NULL], [1, 2, 3, NULL]));
select '-------';
SELECT arraySort(arrayUnion([1, 1, 1, 2, 3], [2, 2, 4], [5, 10, 20]));
select '-------';
SELECT arraySort(arrayUnion([1, 2], [1, 3], [])),
