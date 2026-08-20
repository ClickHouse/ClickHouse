-- Test SQL standard VALUES clause: FROM (VALUES (...), ...) AS t(col, ...)
-- https://github.com/ClickHouse/ClickHouse/issues/99605

SET enable_analyzer = 1;

-- Basic VALUES with column aliases
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val) ORDER BY id;

-- VALUES without explicit column aliases (auto-generated c1, c2, ...)
SELECT * FROM (VALUES (1, 'hello'), (2, 'world')) ORDER BY c1;

-- Single column VALUES
SELECT * FROM (VALUES (10), (20), (30)) AS t(x) ORDER BY x;

-- VALUES in CTE
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte ORDER BY id;

-- VALUES in JOIN
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id
ORDER BY t1.id;

-- VALUES with different types
SELECT * FROM (VALUES (1, 3.14, 'text'), (2, 2.72, 'more')) AS t(id, fval, sval) ORDER BY id;

-- Single row VALUES
SELECT * FROM (VALUES (42, 'answer')) AS t(id, val);

-- Edge case: first-row single-string literal must be treated as row data, not as values structure
SELECT * FROM (VALUES ('x UInt8'), ('hello')) AS t(val) ORDER BY val;
