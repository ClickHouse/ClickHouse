select startsWith([], []);
select startsWith([1], []);
select startsWith([], [1]);
select '-'; 

select startsWith([NULL], [NULL]);
select startsWith([NULL], []);
select startsWith([], [NULL]);
select startsWith([NULL, 1], [NULL]);
select startsWith([NULL, 1], [1]);
select '-'; 

select startsWith([1, 2, 3, 4], [1, 2, 3]);
select startsWith([1, 2, 3, 4], [1, 2, 4]);
select startsWith(['a', 'b', 'c'], ['a', 'b']);
select startsWith(['a', 'b', 'c'], ['b']);
select '-'; 

select endsWith([], []);
select endsWith([1], []);
select endsWith([], [1]);
select '-'; 

select endsWith([NULL], [NULL]);
select endsWith([NULL], []);
select endsWith([], [NULL]);
select endsWith([1, NULL], [NULL]);
select endsWith([NULL, 1], [NULL]);
select '-'; 

select endsWith([1, 2, 3, 4], [3, 4]);
select endsWith([1, 2, 3, 4], [3]);
select '-'; 

select startsWith([1], emptyArrayUInt8());
select endsWith([1], emptyArrayUInt8());
