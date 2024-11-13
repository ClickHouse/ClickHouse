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
SELECT arraySort(arrayUnion([1, 2], [1, 3], []));
select '-------';
-- example from docs
SELECT
    arrayUnion([-2, 1], [10, 1], [-2], []) as num_example,
    arrayUnion(['hi'], [], ['hello', 'hi']) as str_example,
    arrayUnion([1, 3, NULL], [2, 3, NULL]) as null_example;
select '-------';
--mix of types
SELECT arrayUnion([1], [-2], [1.1, 'hi'], [NULL, 'hello', []]);  -- {serverError NO_COMMON_TYPE}
select '-------';
SELECT arrayUnion([1]);
SELECT arrayUnion(); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select '-------';
--bigger arrays
SELECT arraySort(arrayUnion(range(1, 256), range(2, 257)));
SELECT length(arrayUnion(range(1, 100000), range(9999, 200000)));
select '-------';
--bigger number of arguments
SELECT arraySort(arrayUnion([1, 2], [1, 3], [1, 4], [1, 5], [1, 6], [1, 7], [1, 8], [1, 9], [1, 10], [1, 11], [1, 12], [1, 13], [1, 14], [1, 15], [1, 16], [1, 17], [1, 18], [1, 19]));
