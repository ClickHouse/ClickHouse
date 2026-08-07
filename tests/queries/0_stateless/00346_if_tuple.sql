SELECT number % 3 = 2 ? (number, toString(number)) : (number * 10, concat('! ', toString(number))) FROM system.numbers LIMIT 10;

SELECT 0 ? (number, toString(number)) : (number * 10, concat('! ', toString(number))) FROM system.numbers LIMIT 10;
SELECT 1 ? (number, toString(number)) : (number * 10, concat('! ', toString(number))) FROM system.numbers LIMIT 10;

SELECT number % 3 = 2 ? (1, 'Hello') : (2, 'World') FROM system.numbers LIMIT 10;
SELECT number % 3 = 2 ? (number, 'Hello') : (0, 'World') FROM system.numbers LIMIT 10;
SELECT number % 3 = 2 ? (number, 'Hello') : (0, toString(exp2(number))) FROM system.numbers LIMIT 10;
