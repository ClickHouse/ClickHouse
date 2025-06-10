select arrayResize([1, 2, 3], 10);
select arrayResize([1, 2, 3], -10);
select arrayResize([1, Null, 3], 10);
select arrayResize([1, Null, 3], -10);
select arrayResize([1, 2, 3, 4, 5, 6], 3);
select arrayResize([1, 2, 3, 4, 5, 6], -3);
select arrayResize([1, 2, 3], 5, 42);
select arrayResize([1, 2, 3], -5, 42);
select arrayResize(['a', 'b', 'c'], 5);
select arrayResize([[1, 2], [3, 4]], 4);
select arrayResize([[1, 2], [3, 4]], -4);
select arrayResize([[1, 2], [3, 4]], 4, [5, 6]);
select arrayResize([[1, 2], [3, 4]], -4, [5, 6]);

-- different types of array elements and default value to fill
select arrayResize([1, 2, 3], 5, 423.56);
