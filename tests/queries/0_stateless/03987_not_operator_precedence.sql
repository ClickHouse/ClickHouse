-- NOT has lower precedence than IS NULL, BETWEEN, LIKE per SQL standard

-- NOT (x) IS NULL should parse as NOT (x IS NULL), not (NOT x) IS NULL
SELECT NOT (0) IS NULL;
SELECT NOT (NULL) IS NULL;

-- NOT (x) IS NOT NULL should parse as NOT (x IS NOT NULL)
SELECT NOT (0) IS NOT NULL;
SELECT NOT (NULL) IS NOT NULL;

-- NOT (x) BETWEEN a AND b should parse as NOT (x BETWEEN a AND b)
SELECT NOT (3) BETWEEN 1 AND 5;
SELECT NOT (10) BETWEEN 1 AND 5;

-- NOT (x) LIKE pattern should parse as NOT (x LIKE pattern)
SELECT NOT ('hello') LIKE 'hello';
SELECT NOT ('hello') LIKE 'world';

-- NOT precedence vs arithmetic: NOT (0) + NOT (0) = NOT(0 + NOT(0)) = NOT(0 + 1) = NOT(1) = 0
SELECT NOT (0) + NOT (0);

-- Basic NOT still works
SELECT NOT (0);
SELECT NOT (1);

-- Explicit function call still works
SELECT not(0);
SELECT not(1);

-- Verify AST structure with formatQuery
SELECT formatQuery('SELECT NOT (x) IS NULL');
SELECT formatQuery('SELECT NOT (x) BETWEEN 1 AND 5');
SELECT formatQuery('SELECT NOT (x) LIKE y');
