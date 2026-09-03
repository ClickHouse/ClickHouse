SET enable_analyzer = 1;

-- Basic usage of tuple with names (parameter syntax)
SELECT tuple('x', 'y')(1, 'hello') AS t, toTypeName(t), t.x, t.y;

-- Multiple values
SELECT tuple('a', 'b', 'c')(42, 3.14, 'world') AS t, toTypeName(t), t.a, t.b, t.c;

-- Different data types
SELECT tuple('d', 'e', 'f', 'g')(toDate('2023-01-01'), toDateTime('2024-01-01 01:01:01'), [1, 2, 3], map('key', 'value')) AS t, toTypeName(t);

-- Constant expressions
SELECT tuple('result1', 'result2')(1+1, 2*2) AS t, toTypeName(t);

-- Nested tuples
SELECT tuple('outer', 'outer_val')(tuple('inner1', 'inner2')(1, 2), 3) AS t, toTypeName(t);

-- Arrays of tuples with names
SELECT [tuple('id', 'name')(1, 'a'), tuple('id', 'name')(2, 'b')] AS arr, toTypeName(arr);

-- Access elements by both index and name
SELECT tuple('a', 'b', 'c')(10, 20, 30) AS t, t.1, t.a, t.2, t.b, t.3, t.c;

-- Using tuple with names in expressions
SELECT tuple('x', 'y')(1, 2).x + tuple('x', 'y')(3, 4).x;

-- Using with arrayMap
SELECT arrayMap(t -> t.x * 2, [tuple('x', 'y')(1, 'a'), tuple('x', 'y')(2, 'b')]);

-- Using tupleElement with named tuples
SELECT tupleElement(tuple('x', 'y')(1, 2), 'x'), tupleElement(tuple('x', 'y')(1, 2), 'y');

-- Integer elements
SELECT tuple('n1', 'n2', 'n3')(100, 200, 300) AS t, toTypeName(t);

-- String elements
SELECT tuple('greeting', 'place', 'g1', 'g2')('hello', 'world', 'foo', 'bar') AS t, toTypeName(t);

-- Float elements
SELECT tuple('f1', 'f2', 'f3')(1.5, 2.5, 3.5) AS t, toTypeName(t);

-- Bool elements
SELECT tuple('b1', 'b2', 'b3')(true, false, toUInt8(1)) AS t, toTypeName(t);

-- Nullable elements
SELECT tuple('n1', 's1')(toNullable(1), toNullable('hello')) AS t, toTypeName(t);

-- Empty string as element
SELECT tuple('empty', 'filled')('', 'value') AS t, toTypeName(t);

-- Mixed numeric types
SELECT tuple('u8', 'i32', 'i64')(toUInt8(255), toInt32(1000), toInt64(1000000)) AS t, toTypeName(t);

-- Using with tupleNames
SELECT tupleNames(tuple('a', 'b', 'c')(1, 2, 3));

-- Empty tuple with empty names
SELECT tuple()();

-- Error: mismatched number of names and values
SELECT tuple('x', 'y')(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Error: name must be String literal - using number
SELECT tuple(42, 43)(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Error: duplicate names
SELECT tuple('x', 'x')(1, 2); -- { serverError BAD_ARGUMENTS }

-- Error: empty string name
SELECT tuple('')(1); -- { serverError BAD_ARGUMENTS }
