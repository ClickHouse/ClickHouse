
SELECT sumMapFiltered([1,2,3])(a,b) FROM values('a Array(Int64), b Array(Int64)',([1, 2, 3], [10, 10, 10]), ([3, 4, 5], [10, 10, 10]),([4, 5, 6], [10, 10, 10]),([6, 7, 8], [10, 10, 10]));
SELECT sumMapFiltered([1,2,3,toInt8(-3)])(a,b) FROM values('a Array(UInt64), b Array(Int64)',([1, 2, 3], [10, 10, 10]), ([3, 4, 5], [10, 10, 10]),([4, 5, 6], [10, 10, 10]),([6, 7, 8], [10, 10, 10]));
