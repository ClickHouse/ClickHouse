-- Tags: no-old-analyzer

-- The `west` self-join from the IEJoin paper
-- (Khayyat et al., "Lightning Fast and Space Efficient Inequality Joins", PVLDB 8(13), 2015, Fig. 2).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS west;
CREATE TABLE west (t_id Int32, time Int32, cost Int32) ENGINE = MergeTree ORDER BY t_id;
INSERT INTO west VALUES (404, 100, 6), (498, 140, 11), (676, 80, 10), (742, 90, 5);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT s1.t_id, s2.t_id FROM west s1 JOIN west s2 ON s1.time > s2.time AND s1.cost < s2.cost) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT s1.t_id, s2.t_id FROM west s1 JOIN west s2 ON s1.time > s2.time AND s1.cost < s2.cost) WHERE explain LIKE '%IEJoinTransform%';

SELECT s1.t_id, s2.t_id FROM west s1 JOIN west s2 ON s1.time > s2.time AND s1.cost < s2.cost ORDER BY ALL;

-- The same join with loose comparisons: pairs with equal keys now qualify as well
SELECT s1.t_id, s2.t_id FROM west s1 JOIN west s2 ON s1.time >= s2.time AND s1.cost <= s2.cost ORDER BY ALL;

DROP TABLE west;
