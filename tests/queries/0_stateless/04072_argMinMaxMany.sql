-- Basic argMaxMany: returns top N args sorted by val descending
SELECT argMaxMany(2)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));

-- Basic argMinMany: returns bottom N args sorted by val ascending
SELECT argMinMany(2)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));

-- N larger than number of rows: return all rows sorted
SELECT argMaxMany(10)(number, number) FROM numbers(3);
SELECT argMinMany(10)(number, number) FROM numbers(3);

-- Single element
SELECT argMaxMany(1)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));
SELECT argMinMany(1)(arg, val) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',3),('c',2)));

-- NULL val values are excluded
SELECT argMaxMany(3)(a, b) FROM (SELECT * FROM VALUES('a String, b Nullable(Int64)', ('x',1),('y',NULL),('z',3),('w',2)));
SELECT argMinMany(3)(a, b) FROM (SELECT * FROM VALUES('a String, b Nullable(Int64)', ('x',1),('y',NULL),('z',3),('w',2)));

-- NULL arg values are included when corresponding val is top
SELECT argMaxMany(2)(a, b) FROM (SELECT * FROM VALUES('a Nullable(String), b Int64', ('x',1),(NULL,3),('z',2)));
SELECT argMinMany(2)(a, b) FROM (SELECT * FROM VALUES('a Nullable(String), b Int64', ('x',1),(NULL,3),('z',2)));

-- Empty input
SELECT argMaxMany(5)(number, number) FROM numbers(0);
SELECT argMinMany(5)(number, number) FROM numbers(0);

-- Numeric types for arg
SELECT argMaxMany(3)(toInt32(number), toFloat64(number)) FROM numbers(5);
SELECT argMinMany(3)(toInt32(number), toFloat64(number)) FROM numbers(5);

-- All equal vals: result length must be N
SELECT length(argMaxMany(2)(arg, val)) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',1),('c',1)));
SELECT length(argMinMany(2)(arg, val)) FROM (SELECT * FROM VALUES('arg String, val UInt64', ('a',1),('b',1),('c',1)));

-- Error: N must be positive
SELECT argMaxMany(0)(number, number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT argMinMany(-1)(number, number) FROM numbers(5); -- { serverError BAD_ARGUMENTS }

-- Error: val type must be comparable
SELECT argMaxMany(2)(number, [1,2]) FROM numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
