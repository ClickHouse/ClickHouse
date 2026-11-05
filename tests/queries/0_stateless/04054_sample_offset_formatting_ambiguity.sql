-- Verify that a query with both SAMPLE and a query-level OFFSET roundtrips
-- through formatting without the OFFSET being confused with SAMPLE OFFSET.

-- FROM-first: OFFSET is after SELECT, so it is a query-level OFFSET.
-- The AST must show Literal UInt64_10 as a child of SelectQuery (query-level offset),
-- NOT as a child of TableExpression (which would mean SAMPLE offset).
EXPLAIN AST FROM system.one SAMPLE 0.5 SELECT 1 OFFSET 10;

-- Verify `formatQuery` produces FROM-first syntax when SAMPLE + query-level OFFSET
-- coexist, so that re-parsing is unambiguous.
SELECT formatQuery('FROM system.one SAMPLE 0.5 SELECT 1 OFFSET 10');

-- When the OFFSET is a SAMPLE offset (SELECT-first), formatting should stay SELECT-first.
SELECT formatQuery('SELECT 1 FROM system.one SAMPLE 0.5 OFFSET 10');
