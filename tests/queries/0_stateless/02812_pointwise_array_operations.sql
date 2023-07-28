SELECT (materialize([1,1]) + materialize([1,4]));
SELECT ([1,2] + [1,4]);
SELECT ([2.5, 1, 3, 10.1] + [2, 4, 9, 0]);
SELECT ([(1,3), (2,9)] + [(10.1, 2.4), (4,12)]);
SELECT ([[1,1],[2]]+[[12,1],[1]]);
SELECT ([1,2]+[1,number]) from numbers(5);
SELECT ([1,2::UInt64]+[1,number]) from numbers(5);
SELECT ([materialize(1),materialize(2),materialize(3)]-[1,2,3]);
