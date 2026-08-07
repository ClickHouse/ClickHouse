explain ast select tupleElement(255, 100);
explain ast select tupleElement((255, 1), 1);
select tupleElement((255, 1), 1);

EXPLAIN AST SELECT tupleElement(*, 2), tupleElement(x, 1) FROM (SELECT arrayJoin([(0,1)]) AS x);
SELECT tupleElement(*, 2), tupleElement(x, 1) FROM (SELECT arrayJoin([(0,1)]) AS x);
