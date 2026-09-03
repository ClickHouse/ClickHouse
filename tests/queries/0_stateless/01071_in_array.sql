select [1, 2] in [1, 2];
select (1, 2) in (1, 2);
select (1, 2) in [(1, 3), (1, 2)];
select [1] in [[1], [2, 3]];
select NULL in NULL;
select ([1], [2]) in ([NULL], [NULL]);
select ([1], [2]) in (([NULL], [NULL]), ([1], [2]));
select ([1], [2]) in [([NULL], [NULL]), ([1], [2])];
