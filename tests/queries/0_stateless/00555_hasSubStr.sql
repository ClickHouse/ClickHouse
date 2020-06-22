select hasSubStr([], []);
select hasSubStr([], [1]);
select hasSubStr([], [NULL]);
select hasSubStr([Null], [Null]);
select hasSubStr([Null], [Null, 1]);
select hasSubStr([1], []);
select hasSubStr([1], [Null]);
select hasSubStr([1, Null], [Null]);
select hasSubStr([1, Null, 3, 4, Null, 5, 7], [3, 4, Null]);
select hasSubStr([1, Null], [3, 4, Null]);
select '-';


select hasSubStr([1], emptyArrayUInt8());
select '-';

select hasSubStr([1, 2, 3, 4], [1, 3]);
select hasSubStr([1, 2, 3, 4], [1, 3, 5]);
select hasSubStr([-128, 1., 512], [1.]);
select hasSubStr([-128, 1.0, 512], [.3]);
select '-';

select hasSubStr(['a'], ['a']);
select hasSubStr(['a', 'b'], ['a', 'c']);
select hasSubStr(['a', 'c', 'b'], ['a', 'c']);
select '-';

select hasSubStr([1], ['a']);
select hasSubStr([[1, 2], [3, 4]], ['a', 'c']);
select hasSubStr([[1, 2], [3, 4], [5, 8]], [[3, 4]]);
select hasSubStr([[1, 2], [3, 4], [5, 8]], [[3, 4], [5, 8]]);
select hasSubStr([[1, 2], [3, 4], [5, 8]], [[1, 2], [5, 8]]);
