SELECT transform(number, [3, 5, 7], [111, 222, 333]) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 7], [111, 222, 333], 9999) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 7], ['hello', 'world', 'abc'], '') FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], ['hello', 'world', 'abc']) FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], ['hello', 'world', 'abc'], '') FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], ['hello', 'world', 'abc'], '-') FROM system.numbers LIMIT 10;
SELECT transform(toString(number), ['3', '5', '7'], [111, 222, 333], 0) FROM system.numbers LIMIT 10;
