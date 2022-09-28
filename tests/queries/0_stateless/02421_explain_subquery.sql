SELECT count() > 3 FROM (EXPLAIN PIPELINE header = 1 SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain LIKE '%Header: number UInt64%';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT trim(explain) == 'Asterisk' FROM (EXPLAIN AST SELECT * FROM system.numbers LIMIT 10) WHERE explain LIKE '%Asterisk%';
