SELECT (materialize([1,1]) + materialize([1,4]));
SELECT ([1,2] + [1,4]);
SELECT ([2.5, 1, 3, 10.1] + [2, 4, 9, 0]);
SELECT ([(1,3), (2,9)] + [(10.1, 2.4), (4,12)]);
SELECT ([[1,1],[2]]+[[12,1],[1]]);
SELECT ([1,2]+[1,number]) from numbers(5);
SELECT ([1,2::UInt64]+[1,number]) from numbers(5);
SELECT ([materialize(1),materialize(2),materialize(3)]-[1,2,3]);
SELECT [(NULL, 256), (NULL, 256)] + [(1., 100000000000000000000.), (NULL, 1048577)];
SELECT ([1,2::UInt64]+[1,number]) from numbers(5);
CREATE TABLE my_table (values Array(Int32)) ENGINE = MergeTree() ORDER BY values;
INSERT INTO my_table (values) VALUES ([12, 3, 1]);
SELECT values - [1,2,3] FROM my_table WHERE arrayExists(x -> x > 5, values);
SELECT ([12,13] % [5,6]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ([2,3,4]-[1,-2,10,29]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
CREATE TABLE a ( x Array(UInt64), y Array(UInt64)) ENGINE = Memory;
INSERT INTO a VALUES ([2,3],[4,5]),([1,2,3], [4,5]),([6,7],[8,9,10]);
SELECT x, y, x+y FROM a; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
