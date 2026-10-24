SELECT * RENAME (`a b` AS `x.y`, c AS z) FROM (SELECT 1 AS `a b`, 2 AS c) FORMAT TSVWithNames;
SELECT * RENAME (a AS x) FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * RENAME a AS x FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * RENAME ((col) -> concat(col, '_x')) FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * RENAME (col -> if(col = 'a', 'x', 'y')) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * RENAME (a + 1 AS x) FROM (SELECT 1 AS a); -- { clientError SYNTAX_ERROR }
SELECT * RENAME (a AS x,) FROM (SELECT 1 AS a); -- { clientError SYNTAX_ERROR }
SELECT * RENAME ((a, b) -> a) FROM (SELECT 1 AS a); -- { clientError SYNTAX_ERROR }
