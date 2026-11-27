-- Tags: no-old-analyzer
-- Tests WindowNode::dumpTreeImpl branches for ORDER BY, Offset frame type
-- (preceding/following), and FRAME BEGIN/END OFFSET sections in the Analyzer
-- (src/Analyzer/WindowNode.cpp lines 40-41, 47-48, 63-79).
-- Previously uncovered because no existing EXPLAIN QUERY TREE test used a
-- ROWS BETWEEN N PRECEDING AND N FOLLOWING frame, which is required to reach
-- BoundaryType::Offset and the hasFrameBeginOffset/hasFrameEndOffset output sections.

SET enable_analyzer = 1;

EXPLAIN QUERY TREE
SELECT sum(number) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
FROM numbers(5);
