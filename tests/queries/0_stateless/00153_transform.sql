SELECT transform(number, [3, 5, 7], [111, 222, 333]) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 7], [111, 222, 333], 9999) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 7], ['hello', 'world', 'abc'], '') FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], ['hello', 'world', 'abc']) FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], ['hello', 'world', 'abc'], '') FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], ['hello', 'world', 'abc'], '-') FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], [111, 222, 333], 0) FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], [111, 222, 333], -1) FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], [111, 222, 333], -1.1) FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], [111, 222.2, 333], 1) FROM system.numbers LIMIT 10;
SELECT transform(1, [2, 3], ['Bigmir)net', 'Google'], 'Остальные') AS title;
SELECT transform(2, [2, 3], ['Bigmir)net', 'Google'], 'Остальные') AS title;
SELECT transform(3, [2, 3], ['Bigmir)net', 'Google'], 'Остальные') AS title;
SELECT transform(4, [2, 3], ['Bigmir)net', 'Google'], 'Остальные') AS title;
SELECT transform('hello', 'wrong', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT transform('hello', ['wrong'], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT transform('hello', ['wrong'], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT transform(tuple(1), ['sdf'], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
