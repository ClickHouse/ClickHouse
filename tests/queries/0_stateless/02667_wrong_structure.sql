SELECT arrayMap(x -> (toLowCardinality(1) + (SELECT 1 WHERE 0)), [1]);
