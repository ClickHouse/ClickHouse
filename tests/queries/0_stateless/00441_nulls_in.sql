SELECT number IN (1, NULL, 3) FROM system.numbers LIMIT 5;
SELECT nullIf(number, 2) IN (1, NULL, 3) FROM system.numbers LIMIT 5;
SELECT nullIf(number, 2) IN (1, 2, 3) FROM system.numbers LIMIT 5;

SELECT number IN (SELECT number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT number IN (SELECT nullIf(number, 2) FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT nullIf(number, 4) IN (SELECT nullIf(number, 2) FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;


SELECT toString(number) IN ('1', NULL, '3') FROM system.numbers LIMIT 5;
SELECT nullIf(toString(number), '2') IN ('1', NULL, '3') FROM system.numbers LIMIT 5;
SELECT nullIf(toString(number), '2') IN ('1', '2', '3') FROM system.numbers LIMIT 5;

SELECT toString(number) IN (SELECT toString(number) FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT toString(number) IN (SELECT nullIf(toString(number), '2') FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT nullIf(toString(number), '4') IN (SELECT nullIf(toString(number), '2') FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;


SELECT (number, -number) IN ((1, -1), (NULL, NULL), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (nullIf(number, 2), -number) IN ((1, -1), (NULL, NULL), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (nullIf(number, 2), -number) IN ((1, -1), (2, -2), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (nullIf(number, 2), -nullIf(number, 2)) IN ((1, -1), (NULL, NULL), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (nullIf(number, 2), -nullIf(number, 2)) IN ((1, -1), (2, -2), (3, -3)) FROM system.numbers LIMIT 5;

SELECT (number, -number) IN (SELECT number, -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (number, -number) IN (SELECT nullIf(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (nullIf(number, 4), -number) IN (SELECT nullIf(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (number, -nullIf(number, 3)) IN (SELECT nullIf(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (nullIf(number, 4), -nullIf(number, 3)) IN (SELECT nullIf(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
