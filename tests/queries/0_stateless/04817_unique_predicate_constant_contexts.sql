SET enable_analyzer = 1;

-- The UNIQUE predicate in contexts that require a plain constant at analysis time
-- (LIMIT/OFFSET, LIMIT BY, window frame offsets, WITH FILL) and in contexts that need
-- a real value even in only_analyze mode (table function and parameterized view
-- arguments). Each context is exercised both directly and through CREATE VIEW,
-- whose validation analyzes the query without executing it.

SELECT 'limit';
SELECT 1 LIMIT UNIQUE((SELECT number FROM numbers(3)));
SELECT 1 LIMIT UNIQUE((SELECT number % 2 FROM numbers(4)));
EXPLAIN SELECT 1 LIMIT UNIQUE((SELECT number FROM numbers(3)));
CREATE VIEW v_limit AS SELECT 1 LIMIT UNIQUE((SELECT number FROM numbers(3)));
SELECT * FROM v_limit;

SELECT 'offset';
SELECT number FROM numbers(3) OFFSET UNIQUE((SELECT number FROM numbers(3)));
CREATE VIEW v_offset AS SELECT number FROM numbers(3) OFFSET UNIQUE((SELECT number FROM numbers(3)));
SELECT * FROM v_offset;

SELECT 'limit by';
SELECT number % 2 AS n FROM numbers(6) ORDER BY number LIMIT UNIQUE((SELECT 1)) BY n;

SELECT 'window frame offset';
CREATE VIEW v_frame AS SELECT number, sum(number) OVER (ORDER BY number ROWS BETWEEN UNIQUE((SELECT number FROM numbers(3))) PRECEDING AND CURRENT ROW) AS s FROM numbers(4);
SELECT * FROM v_frame;

SELECT 'with fill';
CREATE VIEW v_fill AS SELECT number FROM numbers(3) ORDER BY number WITH FILL FROM UNIQUE((SELECT 5)) TO 6;
SELECT * FROM v_fill;

SELECT 'table function argument';
SELECT * FROM numbers(UNIQUE(SELECT number FROM numbers(3)));
EXPLAIN SELECT * FROM numbers(UNIQUE(SELECT number FROM numbers(3)));
CREATE VIEW v_table_function AS SELECT * FROM numbers(UNIQUE(SELECT number FROM numbers(3)));
SELECT * FROM v_table_function;

SELECT 'parameterized view argument';
CREATE VIEW v_parameterized AS SELECT number FROM numbers({cnt:UInt64});
SELECT * FROM v_parameterized(cnt = UNIQUE(SELECT number FROM numbers(3)));
CREATE VIEW v_parameterized_analyze AS SELECT * FROM v_parameterized(cnt = UNIQUE(SELECT 1));
SELECT * FROM v_parameterized_analyze;
