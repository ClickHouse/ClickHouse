SET enable_analyzer = 1;
-- Test that EXPLAIN SYNTAX formats all operators as functions
-- Related to issue #94603
-- Tests should output function notation (e.g., plus(1, 2)) instead of operator notation (e.g., 1 + 2)

SELECT 'Basic arithmetic operators';
EXPLAIN SYNTAX SELECT 1 + 2;
EXPLAIN SYNTAX SELECT 5 - 3;
EXPLAIN SYNTAX SELECT 4 * 7;
EXPLAIN SYNTAX SELECT 10 / 2;
EXPLAIN SYNTAX SELECT 10 % 3;

SELECT 'Unary operators';
EXPLAIN SYNTAX SELECT -5;
EXPLAIN SYNTAX SELECT NOT true;

SELECT 'Comparison operators';
EXPLAIN SYNTAX SELECT 1 = 1;
EXPLAIN SYNTAX SELECT 1 != 2;
EXPLAIN SYNTAX SELECT 1 <> 2;
EXPLAIN SYNTAX SELECT 1 < 2;
EXPLAIN SYNTAX SELECT 2 > 1;
EXPLAIN SYNTAX SELECT 1 <= 2;
EXPLAIN SYNTAX SELECT 2 >= 1;

SELECT 'Logical operators';
EXPLAIN SYNTAX SELECT true AND false;
EXPLAIN SYNTAX SELECT true OR false;

SELECT 'Nested arithmetic operators';
EXPLAIN SYNTAX SELECT 1 + 2 + 3;
EXPLAIN SYNTAX SELECT 1 * 2 * 3;
EXPLAIN SYNTAX SELECT (1 + 2) * 3;
EXPLAIN SYNTAX SELECT 1 + (2 * 3);

SELECT 'Mixed operators with precedence';
EXPLAIN SYNTAX SELECT 1 + 2 * 3;
EXPLAIN SYNTAX SELECT (1 + 2) * (3 + 4);
EXPLAIN SYNTAX SELECT 10 / 2 - 3;

SELECT 'Complex multi-operator expressions';
EXPLAIN SYNTAX SELECT 1 + 2 * 3, 5 - 1, NOT true, -10;
EXPLAIN SYNTAX SELECT (1 = 1) AND (2 < 3);
EXPLAIN SYNTAX SELECT NOT (1 = 2);

SELECT 'Nested unary operators';
EXPLAIN SYNTAX SELECT NOT NOT true;
EXPLAIN SYNTAX SELECT -(-5);
EXPLAIN SYNTAX SELECT NOT NOT NOT false;

SELECT 'LIKE operator';
EXPLAIN SYNTAX SELECT 'test' LIKE 'te%';
EXPLAIN SYNTAX SELECT 'test' NOT LIKE 'x%';

SELECT 'IN operator';  
EXPLAIN SYNTAX SELECT 1 IN (1, 2, 3);
EXPLAIN SYNTAX SELECT 1 NOT IN (4, 5, 6);

SELECT 'IS NULL operator';
EXPLAIN SYNTAX SELECT NULL IS NULL;
EXPLAIN SYNTAX SELECT 1 IS NOT NULL;

SELECT 'Array subscript operator';
EXPLAIN SYNTAX SELECT [1, 2, 3][1];

SELECT 'Tuple element access';
EXPLAIN SYNTAX SELECT (1, 2).1;

SELECT 'Complex deeply nested expression';
EXPLAIN SYNTAX SELECT ((1 + 2) * (3 - 4)) / ((5 + 6) - (7 * 8));

SELECT 'Mixed logical and comparison';
EXPLAIN SYNTAX SELECT (1 < 2) AND (3 > 2) OR (4 = 4);

SELECT 'All common operators in one query';
EXPLAIN SYNTAX SELECT 
    1 + 2,
    3 - 4, 
    5 * 6,
    7 / 8,
    9 % 2,
    -10,
    NOT true,
    11 = 11,
    12 != 13,
    14 < 15,
    16 > 15,
    17 <= 18,
    19 >= 18,
    true AND false,
    true OR false;

SELECT 'Edge case: operator with column reference';
EXPLAIN SYNTAX SELECT number + 1 FROM system.numbers LIMIT 1;

SELECT 'Edge case: operators in WHERE clause';
EXPLAIN SYNTAX SELECT * FROM system.numbers WHERE number > 0 AND number < 10 LIMIT 1;

SELECT 'Edge case: operators in GROUP BY';
EXPLAIN SYNTAX SELECT number + 1 AS n FROM system.numbers GROUP BY n LIMIT 1;
