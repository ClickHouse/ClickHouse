-- Tags: no-old-analyzer

-- Only a subset of the columns is selected from the join, so the unused ones must be pruned
-- correctly; results are verified against the same query with IEJoin disabled.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS df;

CREATE TABLE df ENGINE = MergeTree ORDER BY tuple() AS
SELECT toInt32(cityHash64(number, 1) % 100 + 1) AS id,
       toInt32(cityHash64(number, 2) % 10 + 1) AS id2,
       toInt32(cityHash64(number, 3) % 5 + 1) AS id3,
       toInt32(cityHash64(number, 4) % 10000) AS v
FROM numbers(3000);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id2, r.v FROM df l JOIN df r ON l.id3 > r.id3 AND l.id3 < r.id3 + 3) WHERE explain LIKE '%IEJoin%';

SELECT (
    SELECT (count(), sum(cityHash64(l.id2, r.v))) FROM df l JOIN df r ON l.id3 > r.id3 AND l.id3 < r.id3 + 3
) = (
    SELECT (count(), sum(cityHash64(l.id2, r.v))) FROM df l JOIN df r ON l.id3 > r.id3 AND l.id3 < r.id3 + 3
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

-- The same with an aggregation on top
SELECT (
    SELECT groupArray((id2, id3, s)) FROM (SELECT l.id2 AS id2, r.id3 AS id3, sum(l.v * r.v) AS s FROM df l JOIN df r ON l.id3 > r.id3 AND l.id3 < r.id3 + 3 GROUP BY id2, id3 ORDER BY id2, id3)
) = (
    SELECT groupArray((id2, id3, s)) FROM (SELECT l.id2 AS id2, r.id3 AS id3, sum(l.v * r.v) AS s FROM df l JOIN df r ON l.id3 > r.id3 AND l.id3 < r.id3 + 3 GROUP BY id2, id3 ORDER BY id2, id3)
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

DROP TABLE df;
