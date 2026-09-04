-- Coverage test for src/Analyzer/SortNode.cpp dumpTreeImpl branches never called by any CI test:
--   line 23  toString(SortDirection::DESCENDING)
--   line 50-51  nulls_sort_direction (ORDER BY ... NULLS FIRST/LAST)
--   line 64-86  WITH FILL FROM/TO/STEP/STALENESS
-- EXPLAIN QUERY TREE calls SortNode::dumpTreeImpl; existing tests only use ASCENDING with no fill options.
-- Tags: no-parallel-replicas

-- 1. DESCENDING sort — hits line 23 (case SortDirection::DESCENDING: return "DESCENDING")
SET enable_analyzer = 1; -- targeted code runs only in the analyzer path; pin it so old-analyzer CI shards behave the same
EXPLAIN QUERY TREE SELECT number FROM numbers(5) ORDER BY number DESC;

-- 2. NULLS FIRST — hits lines 50-51 (nulls_sort_direction branch)
-- ORDER BY ... ASC NULLS FIRST sets nulls_sort_direction = DESCENDING in the SortNode
EXPLAIN QUERY TREE SELECT number FROM numbers(5) ORDER BY number ASC NULLS FIRST;

-- 3. WITH FILL FROM/TO/STEP — hits lines 64-80 (hasFillFrom, hasFillTo, hasFillStep branches)
EXPLAIN QUERY TREE SELECT number FROM numbers(10) ORDER BY number WITH FILL FROM 1 TO 5 STEP 1;

-- 4. WITH FILL STALENESS — hits lines 82-86 (hasFillStaleness branch)
EXPLAIN QUERY TREE SELECT number FROM numbers(10) ORDER BY number WITH FILL FROM 0 TO 5 STEP 1 STALENESS 2;
