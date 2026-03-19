-- Reproducer for exception in PlannerActionsVisitor when a constant is folded
-- from a UNION/EXCEPT ALL subquery (the source expression is a UnionNode).
SELECT (SELECT toUInt256(7)) EXCEPT ALL SELECT 0 EXCEPT ALL SELECT 2147483646;
