SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

-- Only aliases written in the query text may name tuple elements. Aliases synthesized by
-- analyzer passes (for example for JOIN USING or generated ARRAY JOIN columns) share the same
-- query-tree alias field, but must not opt a positional tuple into the named form, because a
-- silently named tuple changes the semantics of a subsequent by-name CAST.
SELECT toTypeName(tuple(a)) FROM (SELECT 1 AS a) t1 JOIN (SELECT 1 AS a) t2 USING (a);
SELECT toTypeName(tuple(u)) FROM (SELECT [1, 2] AS arr) ARRAY JOIN arr AS u;
SELECT toTypeName(tuple(y)) FROM (SELECT 1 AS x) ARRAY JOIN [x + 1] AS y;

-- An explicit alias on the tuple argument itself still names the element.
SELECT toTypeName(tuple(a AS named)) FROM (SELECT 1 AS a) t1 JOIN (SELECT 1 AS a) t2 USING (a);
