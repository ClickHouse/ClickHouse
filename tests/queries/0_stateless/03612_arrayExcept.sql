-- Tests for arrayExcept function
-- arrayExcept returns elements from first array that are not in any subsequent arrays

drop table if exists array_except;

create table array_except (date Date, arr Array(UInt8)) engine=MergeTree partition by date order by date;

insert into array_except values ('2019-01-01', [1,2,3]);
insert into array_except values ('2019-01-01', [1,2]);
insert into array_except values ('2019-01-01', [1]);
insert into array_except values ('2019-01-01', []);

select arraySort(arrayExcept(arr, [1,2])) from array_except order by arr;
select '-------';
select arraySort(arrayExcept(arr, CAST([], 'Array(UInt8)'))) from array_except order by arr;
select '-------';
select arraySort(arrayExcept(CAST([], 'Array(UInt8)'), arr)) from array_except order by arr;
select '-------';
select arraySort(arrayExcept([1,2], arr)) from array_except order by arr;
select '-------';
select arraySort(arrayExcept([1,2,3,4], [1,2])) from array_except order by arr;
select '-------';
select arraySort(arrayExcept(CAST([], 'Array(UInt8)'), CAST([], 'Array(UInt8)'))) from array_except order by arr;

optimize table array_except;

select arraySort(arrayExcept(arr, [1,2])) from array_except order by arr;
select '-------';
select arraySort(arrayExcept(arr, CAST([], 'Array(UInt8)'))) from array_except order by arr;
select '-------';
select arraySort(arrayExcept(CAST([], 'Array(UInt8)'), arr)) from array_except order by arr;
select '-------';
select arraySort(arrayExcept([1,2], arr)) from array_except order by arr;
select '-------';
select arraySort(arrayExcept([1,2,3,4], [1,2])) from array_except order by arr;
select '-------';
select arraySort(arrayExcept(CAST([], 'Array(UInt8)'), CAST([], 'Array(UInt8)'))) from array_except order by arr;

drop table if exists array_except;

select '-------';
-- Basic examples
select arraySort(arrayExcept([1, 2, 3], [2, 3]));
select '-------';
select arraySort(arrayExcept([1, 2, 3], [2, 3], [4]));
select '-------';
select arraySort(arrayExcept([1, 2, 3, 4, 5], [2], [3, 4]));
select '-------';
-- Negative numbers
select arraySort(arrayExcept([-100, 1], [156]));
select '-------';
select arraySort(arrayExcept([1, -2, 3], [-2, 3]));
select '-------';
-- Strings
select arraySort(arrayExcept(['hi', 'hello'], ['hello']));
select '-------';
select arraySort(arrayExcept(['hi', 'hello', 'world'], ['hello', 'world']));
select '-------';
-- NULL handling
SELECT arraySort(arrayExcept([1, 2, NULL], [2]));
select '-------';
SELECT arraySort(arrayExcept([1, 2, NULL], [2, NULL]));
select '-------';
SELECT arraySort(arrayExcept([NULL, 1, 2], [NULL]));
select '-------';
SELECT arraySort(arrayExcept([1, NULL, 2, NULL], [2]));
select '-------';
-- Duplicates in first array
SELECT arraySort(arrayExcept([1, 1, 1, 2, 3], [2, 4]));
select '-------';
SELECT arraySort(arrayExcept([2, 2, 3, 3], [2]));
select '-------';
-- Empty arrays
SELECT arraySort(arrayExcept([1, 2], CAST([], 'Array(Int32)')));
select '-------';
SELECT arraySort(arrayExcept(CAST([], 'Array(Int32)'), [1, 2]));
select '-------';
SELECT arraySort(arrayExcept(CAST([], 'Array(Int32)'), CAST([], 'Array(Int32)')));
select '-------';
-- Single element
SELECT arraySort(arrayExcept([1], [2]));
select '-------';
SELECT arraySort(arrayExcept([1], [1]));
select '-------';
-- Multiple arrays
SELECT arraySort(arrayExcept([1, 2, 3, 4, 5], [2], [3], [4]));
select '-------';
SELECT arraySort(arrayExcept([1, 2, 3], [1], [2], [3]));
select '-------';
-- Example similar to Snowflake docs
SELECT
    arraySort(arrayExcept([1, 2, 3], [2, 3])) as basic_example,
    arraySort(arrayExcept([1, 2, 3], [2, 3], [4])) as multiple_arrays,
    arraySort(arrayExcept([1, 2, NULL], [2, NULL])) as with_null;
select '-------';
-- Edge cases
SELECT arraySort(arrayExcept([1], [1, 2, 3]));
select '-------';
SELECT arraySort(arrayExcept([1, 2, 3], [1, 2, 3]));
select '-------';
-- Larger arrays
SELECT arraySort(arrayExcept(range(1, 101), range(50, 101)));
select '-------';
SELECT length(arrayExcept(range(1, 1000), range(500, 1000)));
select '-------';
-- Multiple arguments
SELECT arraySort(arrayExcept([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [2], [4], [6], [8], [10]));
select '-------';



