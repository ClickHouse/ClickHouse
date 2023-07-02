drop table if exists array_jaccard_index;

create table array_jaccard_index (arr Array(UInt8)) engine = MergeTree order by arr;

insert into array_jaccard_index values ([1,2,3]);

insert into array_jaccard_index values ([1,2]);

insert into array_jaccard_index values ([1]);

select arr as arr_1, [1,2] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) from array_jaccard_index order by arr;

select arr as arr_1, [] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) from array_jaccard_index order by arr;

select [] as arr_1, arr as arr_2,  round(arrayJaccardIndex(arr_1, arr_2), 2) from array_jaccard_index order by arr;

select [1,2] as arr_1, arr as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) from array_jaccard_index order by arr;

select arr as arr_1, arr as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2) from array_jaccard_index order by arr;

drop table array_jaccard_index;

select ['a'] as arr_1, ['a', 'aa', 'aaa'] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);

select [1, 1.1, 2.2] as arr_1, [2.2, 3.3, 444] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);

select [toUInt16(1)] as arr_1, [toUInt32(1)] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);

select [1,2] as arr_1, [1,2,3,4] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);

select [[1,2], [3,4]] as arr_1, [[1,2], [3,5]] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2);
