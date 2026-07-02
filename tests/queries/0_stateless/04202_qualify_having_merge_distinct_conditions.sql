-- Test: HAVING + QUALIFY with no window functions - QUALIFY is merged into HAVING with AND.
-- Covers: src/Planner/Planner.cpp:1975-1982 — when select_query_info.has_window is false
-- and query_node has both HAVING and QUALIFY, mergeConditionNodes builds AND(HAVING, QUALIFY).
-- Existing tests in 03095_window_functions_qualify.sql use IDENTICAL conditions for HAVING and QUALIFY,
-- so they cannot detect a mutation that drops one side, swaps to OR, or returns only one operand.

SET enable_analyzer = 1;

-- HAVING `key < 3` keeps keys 0,1,2. QUALIFY `key % 2 = 1` keeps keys 1,3.
-- Correct AND merge: only key=1 survives (count=3).
-- Drop QUALIFY: 3 rows {0,1,2}. Drop HAVING: 2 rows {1,3}. OR: 4 rows {0,1,2,3}.
SELECT (number % 5) AS key, count() AS c
FROM numbers(15)
GROUP BY key
HAVING key < 3
QUALIFY key % 2 = 1
ORDER BY key;

-- Symmetric test to defeat any heuristic that picks one side.
-- HAVING `key % 2 = 0` keeps keys 0,2,4. QUALIFY `key < 3` keeps keys 0,1,2.
-- Correct AND merge: keys 0,2.
SELECT (number % 5) AS key, count() AS c
FROM numbers(15)
GROUP BY key
HAVING key % 2 = 0
QUALIFY key < 3
ORDER BY key;

-- Empty intersection — HAVING and QUALIFY have disjoint filters.
-- HAVING `key < 2` keeps {0,1}. QUALIFY `key > 3` keeps {4}. Intersection: empty.
SELECT (number % 5) AS key, count() AS c
FROM numbers(15)
GROUP BY key
HAVING key < 2
QUALIFY key > 3
ORDER BY key;
