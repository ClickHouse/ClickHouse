select startsWith([], []);
select startsWith([1], []);
select startsWith([], [1]);

select startsWith([NULL], [NULL]);
select startsWith([NULL], []);
select startsWith([], [NULL]);

select startsWith([1, 2, 3, 4], [1, 2, 3]);
select startsWith([1, 2, 3, 4], [1, 2, 4]);

select startsWith(['a', 'b', 'c'], ['a', 'b']);
select startsWith(['a', 'b', 'c'], ['b']);
