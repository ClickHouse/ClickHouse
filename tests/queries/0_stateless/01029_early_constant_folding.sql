SET enable_debug_queries = 1;

-- constant folding

ANALYZE SELECT 1 WHERE 1 = 0;

ANALYZE SELECT 1 WHERE 1 IN (0, 1, 2);

ANALYZE SELECT 1 WHERE 1 IN (0, 2) AND 2 = (SELECT 2);

-- no constant folding

ANALYZE SELECT 1 WHERE 1 IN ((SELECT arrayJoin([1, 2, 3])) AS subquery);

ANALYZE SELECT 1 WHERE NOT ignore();
