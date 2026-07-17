-- Tags: no-old-analyzer

-- The East/West example queries from the IEJoin paper (Khayyat et al., PVLDB 8(13)).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS east;
DROP TABLE IF EXISTS west;
DROP TABLE IF EXISTS weststr;

CREATE TABLE east (rid String, id Int32, dur Int32, rev Int32, cores Int32) ENGINE = MergeTree ORDER BY rid;
CREATE TABLE west (rid String, t_id Int32, time Int32, cost Int32, cores Int32) ENGINE = MergeTree ORDER BY rid;
INSERT INTO east VALUES ('r1', 100, 140, 12, 2), ('r2', 101, 100, 12, 8), ('r3', 103, 90, 5, 4);
INSERT INTO west VALUES ('s1', 404, 100, 6, 4), ('s2', 498, 140, 11, 2), ('s3', 676, 80, 10, 1), ('s4', 742, 90, 5, 4);

-- Qs: a single inequality condition is not IEJoin territory, it is executed as a cross join with a filter
SELECT count() FROM (EXPLAIN actions = 1 SELECT s1.rid, s2.rid FROM west s1 JOIN west s2 ON s1.time > s2.time) WHERE explain LIKE '%IEJoin%';
SELECT s1.rid, s2.rid FROM west s1 JOIN west s2 ON s1.time > s2.time ORDER BY 1, 2;

-- Qp
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT s1.rid, s2.rid FROM west s1 JOIN west s2 ON s1.time > s2.time AND s1.cost < s2.cost) WHERE explain LIKE '%IEJoin%';
SELECT s1.rid, s2.rid FROM west s1 JOIN west s2 ON s1.time > s2.time AND s1.cost < s2.cost ORDER BY 1, 2;

-- Qt
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT east.rid, west.rid FROM east JOIN west ON east.dur < west.time AND east.rev > west.cost) WHERE explain LIKE '%IEJoin%';
SELECT east.rid, west.rid FROM east JOIN west ON east.dur < west.time AND east.rev > west.cost ORDER BY 1, 2;

-- String comparisons
CREATE TABLE weststr ENGINE = MergeTree ORDER BY rid AS SELECT rid, toString(time) AS time, toString(cost) AS cost FROM west;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT s1.rid, s2.rid FROM weststr s1 JOIN weststr s2 ON s1.time > s2.time AND s1.cost < s2.cost) WHERE explain LIKE '%IEJoin%';
SELECT s1.rid, s2.rid FROM weststr s1 JOIN weststr s2 ON s1.time > s2.time AND s1.cost < s2.cost ORDER BY 1, 2;

DROP TABLE east;
DROP TABLE west;
DROP TABLE weststr;
