select 1 in tuple(1, 2, 3, 4, 5) settings max_temporary_columns = 2;
select (1, 2) in tuple(tuple(1, 2), tuple(3, 4), tuple(5, 6), tuple(7, 8), tuple(9, 10)) settings max_temporary_columns = 4;

select 1 in array(1, 2, 3, 4, 5) settings max_temporary_columns = 3;
select (1, 2) in array(tuple(1, 2), tuple(3, 4), tuple(5, 6), tuple(7, 8), tuple(9, 10)) settings max_temporary_columns = 4;

select (1, 2) in tuple(1, 2);
select (1, 2) in array((1, 3), (1, 2));
select [1] in array([1], [2, 3]);
select ([1], [2]) in tuple([NULL], [NULL]);
select ([1], [2]) in tuple(([NULL], [NULL]), ([1], [2]));

select 4 in plus(2, 2);
select (1, 'a') in tuple((1, 'a'), (2, 'b'), (3, 'c'));
select (1, 'a') in tuple((2, 'b'), (3, 'c'), (4, 'd'));
select (1, (2, 'foo')) in tuple((1, (3, 'b')), (1, (2, 'foo')));
