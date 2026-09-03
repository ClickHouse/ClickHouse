SET join_use_nulls = 1;

SELECT b.id
FROM (
    SELECT toLowCardinality(0 :: UInt32) AS id
    GROUP BY []
) AS a
SEMI LEFT JOIN (
    SELECT toLowCardinality(1 :: UInt64) AS id
) AS b
USING (id);
