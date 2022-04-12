-- CAST expression

SELECT cast('1234' AS lhs, 'UInt32' AS rhs), lhs, rhs;
SELECT cast('1234' lhs, 'UInt32' rhs), lhs, rhs;
SELECT cast('1234' lhs AS UInt32), lhs;
SELECT cast('1234' AS lhs AS UInt32), lhs;

-- SUBSTRING expression

-- SUBSTRING(expr, start, length)

SELECT substring('1234' AS arg_1, 2 AS arg_2, 3 AS arg_3), arg_1, arg_2, arg_3;
SELECT substring('1234' arg_1, 2 arg_2, 3 arg_3), arg_1, arg_2, arg_3;

-- SUBSTRING(expr FROM start)

SELECT substring('1234' AS arg_1 FROM 2 AS arg_2), arg_1, arg_2;
SELECT substring('1234' arg_1 FROM 2 arg_2), arg_1, arg_2;

-- SUBSTRING(expr FROM start FOR length)

SELECT substring('1234' AS arg_1 FROM 2 AS arg_2 FOR 3 AS arg_3), arg_1, arg_2, arg_3;
SELECT substring('1234' arg_1 FROM 2 arg_2 FOR 3 arg_3), arg_1, arg_2, arg_3;


-- TRIM expression ([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)

SELECT trim(LEADING 'a' AS arg_1 FROM 'abca' AS arg_2), arg_1, arg_2;
SELECT trim(LEADING 'a' arg_1 FROM 'abca' arg_2), arg_1, arg_2;

SELECT trim(TRAILING 'a' AS arg_1 FROM 'abca' AS arg_2), arg_1, arg_2;
SELECT trim(TRAILING 'a' arg_1 FROM 'abca' arg_2), arg_1, arg_2;

SELECT trim(BOTH 'a' AS arg_1 FROM 'abca' AS arg_2), arg_1, arg_2;
SELECT trim(BOTH 'a' arg_1 FROM 'abca' arg_2), arg_1, arg_2;

-- EXTRACT expression

-- EXTRACT(part FROM date)

SELECT EXTRACT(DAY FROM toDate('2019-05-05') as arg_1), arg_1;
SELECT EXTRACT(DAY FROM toDate('2019-05-05') arg_1), arg_1;

-- Function extract(haystack, pattern)

SELECT extract('1234' AS arg_1, '123' AS arg_2), arg_1, arg_2;
SELECT extract('1234' arg_1, '123' arg_2), arg_1, arg_2;

-- POSITION expression

-- position(needle IN haystack)

SELECT position(('123' AS arg_1) IN ('1234' AS arg_2)), arg_1, arg_2;

-- position(haystack, needle[, start_pos])

SELECT position('123' AS arg_1, '1234' AS arg_2), arg_1, arg_2;
SELECT position('123' arg_1, '1234' arg_2), arg_1, arg_2;

-- dateAdd, dateSub expressions

-- function(unit, offset, timestamp)

SELECT dateAdd(DAY, 1 AS arg_1, toDate('2019-05-05') AS arg_2), arg_1, arg_2;
SELECT dateAdd(DAY, 1 arg_1, toDate('2019-05-05') arg_2), arg_1, arg_2;

-- function(offset, timestamp)

SELECT dateAdd(DAY, 1 AS arg_1, toDate('2019-05-05') AS arg_2), arg_1, arg_2;
SELECT dateAdd(DAY, 1 arg_1, toDate('2019-05-05') arg_2), arg_1, arg_2;

-- function(unit, offset, timestamp)

SELECT dateSub(DAY, 1 AS arg_1, toDate('2019-05-05') AS arg_2), arg_1, arg_2;
SELECT dateSub(DAY, 1 arg_1, toDate('2019-05-05') arg_2), arg_1, arg_2;

-- function(offset, timestamp)

SELECT dateSub(DAY, 1 AS arg_1, toDate('2019-05-05') AS arg_2), arg_1, arg_2;
SELECT dateSub(DAY, 1 arg_1, toDate('2019-05-05') arg_2), arg_1, arg_2;

-- dateDiff expression

-- dateDiff(unit, startdate, enddate, [timezone])

SELECT dateDiff(DAY, toDate('2019-05-05') AS arg_1, toDate('2019-05-06') AS arg_2), arg_1, arg_2;
SELECT dateDiff(DAY, toDate('2019-05-05') arg_1, toDate('2019-05-06') arg_2), arg_1, arg_2;

-- dateDiff('unit', startdate, enddate, [timezone])

SELECT dateDiff('DAY', toDate('2019-05-05') AS arg_1, toDate('2019-05-06') AS arg_2), arg_1, arg_2;
SELECT dateDiff('DAY', toDate('2019-05-05') arg_1, toDate('2019-05-06') arg_2), arg_1, arg_2;
