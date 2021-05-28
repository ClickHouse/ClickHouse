WITH 8.5 AS a, 2.5 AS b SELECT a % b, -a % b, a % -b, -a % -b;
WITH 10.125 AS a, 2.5 AS b SELECT a % b, -a % b, a % -b, -a % -b;
WITH 8.5 AS a, 2.5 AS b SELECT mod(a, b), MOD(-a, b), modulo(a, -b), moduloOrZero(-a, -b);
