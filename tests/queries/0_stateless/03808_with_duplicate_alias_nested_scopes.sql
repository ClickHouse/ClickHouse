-- Test for duplicate alias handling in nested scopes with WITH clause.
-- The issue was that when global_with_aliases is copied between scopes,
-- the QueryTreeNodePtr smart pointers reference the same underlying nodes.
-- If a child scope processes a duplicate and removes its alias,
-- the parent scope's reference to the same node would have an empty alias,
-- causing a logical error.

SET enable_analyzer = 1;
SET enable_scopes_for_with_statement = 0;

-- The problematic query found by BuzzHouse fuzzer
SELECT c0 FROM (WITH CAST('11', 'Int128') AS a1 SELECT c0 FROM (WITH CAST('11', 'Int128') AS a1 SELECT CAST('38', 'UInt16') AS a0) (c0));

-- Simpler reproducer with nested WITH clauses having the same alias
WITH 1 AS x SELECT * FROM (WITH 1 AS x SELECT 1 AS col) AS subq;

-- Another variant
WITH CAST(1, 'UInt8') AS a SELECT * FROM (WITH CAST(1, 'UInt8') AS a SELECT 2 AS b) (b);
