-- Reproducer for exception in PlannerActionsVisitor when a constant is folded
-- from a UNION/EXCEPT ALL subquery (the source expression is a UnionNode).
-- The remote() call forces SECONDARY_QUERY context where isASTLevelOptimizationAllowed() is false,
-- which is needed to reach the buggy code path.
SELECT ((SELECT toUInt256(7)) EXCEPT ALL SELECT 0 EXCEPT ALL SELECT 2147483646) FROM remote('127.0.0.1', numbers(1));
