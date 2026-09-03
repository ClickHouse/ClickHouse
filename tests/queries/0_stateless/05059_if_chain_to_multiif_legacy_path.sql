-- Coverage test for OptimizeIfChainsVisitor (old AST path, bypassed when enable_analyzer=1)
-- Exercises OptimizeIfChains.cpp lines 36-57 (if-chain detection and rewrite)
-- and lines 61-93 (ifChain: recursive argument collection).
-- Tags: no-parallel-replicas

SET enable_analyzer = 0;
SET optimize_if_chain_to_multiif = 1;

-- 1. Two-level chain: if(a, x, if(b, y, z)) → multiIf(a, x, b, y, z)
EXPLAIN SYNTAX SELECT if(number = 1, 'one', if(number = 2, 'two', 'other')) FROM numbers(4) ORDER BY number;
SELECT if(number = 1, 'one', if(number = 2, 'two', 'other')) FROM numbers(4) ORDER BY number;

-- 2. Three-level chain: if(a, x, if(b, y, if(c, z, w))) → multiIf(a, x, b, y, c, z, w)
-- Exercises recursive ifChain (lines 79-85 in OptimizeIfChains.cpp)
EXPLAIN SYNTAX SELECT if(number = 1, 'one', if(number = 2, 'two', if(number = 3, 'three', 'other'))) FROM numbers(5) ORDER BY number;
SELECT if(number = 1, 'one', if(number = 2, 'two', if(number = 3, 'three', 'other'))) FROM numbers(5) ORDER BY number;

-- 3. Simple if (no nesting) is NOT rewritten to multiIf
-- Disable enum-transform to keep EXPLAIN SYNTAX output stable across randomized settings
SET optimize_if_transform_strings_to_enum = 0;
EXPLAIN SYNTAX SELECT if(number = 1, 'one', 'other') FROM numbers(3) ORDER BY number;
