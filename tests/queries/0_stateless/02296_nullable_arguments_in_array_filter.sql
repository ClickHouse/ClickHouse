select arrayFilter(x -> 2 * x > 0, []);
select arrayFilter(x -> 2 * x > 0, [NULL]);
select arrayFilter(x -> x % 2 ? NULL : 1, [1, 2, 3, 4]);
select arrayFilter(x -> x % 2, [1, NULL, 3, NULL]);
