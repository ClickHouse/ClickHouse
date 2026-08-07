-- Test for duplicate alias handling in nested scopes with WITH clause.
-- The issue requires 3 levels of nesting:
-- 1. Outer level with WITH alias X
-- 2. Middle level with same WITH alias X (creates duplicate in middle scope)
-- 3. Inner level that inherits the duplicate, processes it, removes the alias
-- When middle scope later processes its duplicates, the alias is already gone.

SET enable_analyzer = 1;
SET enable_scopes_for_with_statement = 0;

-- Three levels of nesting with the same WITH alias at each level
SELECT * FROM (
    WITH CAST('11', 'Int128') AS a1
    SELECT c0 FROM (
        WITH CAST('11', 'Int128') AS a1
        SELECT c0 FROM (
            WITH CAST('11', 'Int128') AS a1
            SELECT CAST('38', 'UInt16') AS a0
        ) (c0)
    ) (c0)
) (c0);

-- Simpler 3-level variant
SELECT * FROM (
    WITH 1 AS x
    SELECT * FROM (
        WITH 1 AS x
        SELECT * FROM (
            WITH 1 AS x
            SELECT 1 AS col
        ) AS t3
    ) AS t2
) AS t1;
