SELECT sumMapFiltered([6.7])([x], [y])
FROM values('x Float64, y Float64', (0, 1), (1, 2.3), (nan, inf), (6.7, 3), (4, 4), (5, 1));

SELECT sumMap([x],[y]) FROM values('x Float64, y Float64', (4, 1), (1, 2.3), (nan,inf), (6.7,3), (4,4), (5, 1));
