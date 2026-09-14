SET enable_analyzer = 1;
-- If you are reading this test please note that as of now this setting does not provide benefits in most of the cases.
SET optimize_if_chain_to_multiif = 0;
EXPLAIN SYNTAX SELECT number = 1 ? 'hello' : (number = 2 ? 'world' : 'xyz') FROM numbers(10);
SET optimize_if_chain_to_multiif = 1;
EXPLAIN SYNTAX SELECT number = 1 ? 'hello' : (number = 2 ? 'world' : 'xyz') FROM numbers(10);

-- fuzzed
SELECT now64(if(Null, NULL, if(Null, nan, toFloat64(number))), Null) FROM numbers(2);

-- Coverage for OptimizeIfChainsVisitor (legacy AST path, bypassed when enable_analyzer=1).
-- Exercises OptimizeIfChains.cpp lines 36-57 (chain detection/rewrite) and
-- lines 61-93 (ifChain: recursive argument collection).
SET enable_analyzer = 0;
SET optimize_if_chain_to_multiif = 1;

-- 1. Two-level chain: if(a, x, if(b, y, z)) → multiIf(a, x, b, y, z)
EXPLAIN SYNTAX SELECT if(number = 1, 'one', if(number = 2, 'two', 'other')) FROM numbers(4) ORDER BY number;
SELECT if(number = 1, 'one', if(number = 2, 'two', 'other')) FROM numbers(4) ORDER BY number;

-- 2. Three-level chain: exercises recursive ifChain (lines 79-85 in OptimizeIfChains.cpp)
EXPLAIN SYNTAX SELECT if(number = 1, 'one', if(number = 2, 'two', if(number = 3, 'three', 'other'))) FROM numbers(5) ORDER BY number;
SELECT if(number = 1, 'one', if(number = 2, 'two', if(number = 3, 'three', 'other'))) FROM numbers(5) ORDER BY number;

-- 3. Simple if (no nesting) is NOT rewritten to multiIf
SET optimize_if_transform_strings_to_enum = 0;
EXPLAIN SYNTAX SELECT if(number = 1, 'one', 'other') FROM numbers(3) ORDER BY number;
