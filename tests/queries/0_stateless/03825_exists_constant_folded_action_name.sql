-- Test for EXISTS subquery that is constant-folded into a ConstantNode.
-- When the planner reconstructs the action node name from the source expression
-- (e.g., in distributed/secondary query context), the EXISTS argument subquery
-- may not have an alias because createUniqueAliasesIfNecessary doesn't visit
-- inside ConstantNode source expressions. This should not cause an exception.

-- Simplified reproduction: EXISTS in a scalar subquery evaluated during analysis.
-- The remote() call forces SECONDARY_QUERY context where isASTLevelOptimizationAllowed() is false.
SELECT exists((SELECT 1)) FROM remote('127.0.0.1', numbers(1));
SELECT exists((SELECT 1)) FROM remote('127.0.0.1', numbers(1)) WHERE exists((SELECT 1));

-- Multiple EXISTS expressions
SELECT exists((SELECT 1)), exists((SELECT 2)) FROM remote('127.0.0.1', numbers(1));

-- EXISTS with more complex subquery
SELECT exists((SELECT number FROM numbers(1))) FROM remote('127.0.0.1', numbers(1));

-- NOT exists
SELECT NOT exists((SELECT 1)) FROM remote('127.0.0.1', numbers(1));
