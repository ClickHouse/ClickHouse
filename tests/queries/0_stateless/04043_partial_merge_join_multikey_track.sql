-- Test compareTrackAt multi-row skipping in partial_merge join with multi-key sort.
-- MergeJoin.cpp:327 uses compareTrackAt on the first sort key to skip N rows at once
-- (via nextN(-cmp) at MergeJoin.cpp:340), then falls back to regular compareAt for
-- secondary keys when the first key is equal. Exercises the interaction between
-- first-key track skipping and secondary-key matching.

SET join_algorithm = 'partial_merge';

DROP TABLE IF EXISTS t_04043_left;
DROP TABLE IF EXISTS t_04043_right;

-- Multi-key tables: (key1, key2)
CREATE TABLE t_04043_left  (key1 UInt32, key2 UInt32, val String) ENGINE = MergeTree() ORDER BY (key1, key2);
CREATE TABLE t_04043_right (key1 UInt32, key2 UInt32, val String) ENGINE = MergeTree() ORDER BY (key1, key2);

-- Left key1:  1,1,2,2,3,5,5,5,10,10 — runs of key1 values less than right's key1
-- force compareTrackAt to return track > 1 on the first key, then test secondary key
INSERT INTO t_04043_left  VALUES (1,10,'L1a'), (1,20,'L1b'), (2,10,'L2a'), (2,20,'L2b'), (3,10,'L3a'), (5,10,'L5a'), (5,20,'L5b'), (5,30,'L5c'), (10,10,'L10a'), (10,20,'L10b');
INSERT INTO t_04043_right VALUES (3,10,'R3a'), (5,20,'R5b'), (5,30,'R5c'), (10,10,'R10a');

-- INNER JOIN on (key1, key2): verify multi-row skip on key1 does not miss key2 matches
SELECT l.key1, l.key2, l.val, r.val
FROM t_04043_left l INNER JOIN t_04043_right r ON l.key1 = r.key1 AND l.key2 = r.key2
ORDER BY l.key1, l.key2;

-- LEFT JOIN: all left rows present, right values only for exact (key1,key2) matches
SELECT l.key1, l.key2, l.val, r.val
FROM t_04043_left l LEFT JOIN t_04043_right r ON l.key1 = r.key1 AND l.key2 = r.key2
ORDER BY l.key1, l.key2;

-- Exact count of inner join matches
SELECT count()
FROM t_04043_left l INNER JOIN t_04043_right r ON l.key1 = r.key1 AND l.key2 = r.key2;

DROP TABLE t_04043_left;
DROP TABLE t_04043_right;
