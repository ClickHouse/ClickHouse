select arrayReduce('leastSqr', [1, 2, 3, 4], [100, 110, 120, 130]);
select arrayReduce('leastSqr', [1, 2, 3, 4], [100, 110, 120, 131]);
select arrayReduce('leastSqr', [-1, -2, -3, -4], [-100, -110, -120, -130]);
select arrayReduce('leastSqr', [5, 5.1], [6, 6.1]);
select arrayReduce('leastSqr', [0], [0]);
select arrayReduce('leastSqr', [3, 4], [3, 3]);
select arrayReduce('leastSqr', [3, 3], [3, 4]);
select arrayReduce('leastSqr', emptyArrayUInt8(), emptyArrayUInt8());

