drop table if exists array_jaccard_index;

create table array_jaccard_index (arr Array(UInt8)) engine=MergeTree partition by arr order by arr;

insert into array_jaccard_index values ([1,2,3]);
insert into array_jaccard_index values ([1,2]);
insert into array_jaccard_index values ([1]);
insert into array_jaccard_index values ([]);

select round(arrayJaccardIndex(arr, [1,2]), 2) from array_jaccard_index order by arr;
select round(arrayJaccardIndex(arr, []), 2) from array_jaccard_index order by arr;
select round(arrayJaccardIndex([], arr), 2) from array_jaccard_index order by arr;
select round(arrayJaccardIndex([1,2], arr), 2) from array_jaccard_index order by arr;
select round(arrayJaccardIndex([1,2], [1,2,3,4]), 2) from array_jaccard_index order by arr;
select round(arrayJaccardIndex([], []), 2) from array_jaccard_index order by arr;
select round(arrayJaccardIndex(arr, arr), 2) from array_jaccard_index order by arr;

drop table if exists array_jaccard_index;

select round(arrayJaccardIndex(['a'], ['a', 'aa', 'aaa']), 2);

select round(arrayJaccardIndex([1, 1.1, 2.2], [2.2, 3.3, 444]), 2);

select round(arrayJaccardIndex([], []), 2);

select round(arrayJaccardIndex([toUInt16(1)], [toUInt32(1)]), 2);
