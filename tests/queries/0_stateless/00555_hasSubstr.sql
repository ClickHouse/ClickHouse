select hasSubstr([], []);
select hasSubstr([], [1]);
select hasSubstr([], [NULL]);
select hasSubstr([Null], [Null]);
select hasSubstr([Null], [Null, 1]);
select hasSubstr([1], []);
select hasSubstr([1], [Null]);
select hasSubstr([1, Null], [Null]);
select hasSubstr([1, Null, 3, 4, Null, 5, 7], [3, 4, Null]);
select hasSubstr([1, Null], [3, 4, Null]);
select '-';


select hasSubstr([1], emptyArrayUInt8());
select '-';

select hasSubstr([1, 2, 3, 4], [1, 3]);
select hasSubstr([1, 2, 3, 4], [1, 3, 5]);
select hasSubstr([-128, 1., 512], [1.]);
select hasSubstr([-128, 1.0, 512], [.3]);
select '-';

select hasSubstr(['a'], ['a']);
select hasSubstr(['a', 'b'], ['a', 'c']);
select hasSubstr(['a', 'c', 'b'], ['a', 'c']);
select '-';

select hasSubstr([1], ['a']);
select hasSubstr([[1, 2], [3, 4]], ['a', 'c']);
select hasSubstr([[1, 2], [3, 4], [5, 8]], [[3, 4]]);
select hasSubstr([[1, 2], [3, 4], [5, 8]], [[3, 4], [5, 8]]);
select hasSubstr([[1, 2], [3, 4], [5, 8]], [[1, 2], [5, 8]]);
