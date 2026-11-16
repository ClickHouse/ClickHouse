-- Tags: no-old-analyzer
-- Tests InterpolateNode::dumpTreeImpl (src/Analyzer/InterpolateNode.cpp lines 24-32).
-- Previously uncovered because no existing EXPLAIN QUERY TREE test used
-- ORDER BY ... WITH FILL ... INTERPOLATE (...), which is the only SQL construct
-- that creates an InterpolateNode in the query tree.

SET enable_analyzer = 1;

EXPLAIN QUERY TREE
SELECT number, val
FROM (SELECT number, number * 2 AS val FROM numbers(5))
ORDER BY number WITH FILL FROM 1 TO 8 INTERPOLATE (val AS val + 1);
