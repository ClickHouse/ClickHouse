-- Tags: no-old-analyzer

-- Non-overlapping ranges must produce an empty result with both orders of the conditions
-- (the second order exercises the short circuit on the second condition).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS overlap_left;
DROP TABLE IF EXISTS overlap_right;

CREATE TABLE overlap_left (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE overlap_right (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO overlap_left VALUES (1, 10), (2, 11), (3, 12), (4, 13);
INSERT INTO overlap_right VALUES (1, 101), (2, 102), (3, 103), (4, 104);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT t1.x, t2.x FROM overlap_left t1 JOIN overlap_right t2 ON t1.x < t2.x AND t1.y > t2.y) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM overlap_left t1 JOIN overlap_right t2 ON t1.x < t2.x AND t1.y > t2.y;

-- Reverse condition order
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT t1.x, t2.x FROM overlap_left t1 JOIN overlap_right t2 ON t1.y > t2.y AND t1.x < t2.x) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM overlap_left t1 JOIN overlap_right t2 ON t1.y > t2.y AND t1.x < t2.x;

DROP TABLE overlap_left;
DROP TABLE overlap_right;
