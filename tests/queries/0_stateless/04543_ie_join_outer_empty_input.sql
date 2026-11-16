-- Tags: no-old-analyzer

-- OUTER/SEMI/ANTI kinds with an empty side: the unmatched post-phase must emit every row of
-- the other side padded with defaults (NULLs with `join_use_nulls`), and nothing otherwise.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS full_side;
DROP TABLE IF EXISTS empty_side;

CREATE TABLE full_side (id Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE empty_side (id Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO full_side VALUES (1, 1, 3), (2, 2, 2), (3, 3, 1);

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM full_side l LEFT JOIN empty_side r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

SELECT 'left of empty right';
SELECT * FROM full_side l LEFT JOIN empty_side r ON l.x < r.x AND l.y > r.y ORDER BY l.id;
SELECT 'right of empty right', count() FROM full_side l RIGHT JOIN empty_side r ON l.x < r.x AND l.y > r.y;
SELECT 'full of empty right';
SELECT * FROM full_side l FULL JOIN empty_side r ON l.x < r.x AND l.y > r.y ORDER BY l.id;
SELECT 'semi of empty right', count() FROM full_side l LEFT SEMI JOIN empty_side r ON l.x < r.x AND l.y > r.y;
SELECT 'anti of empty right';
SELECT * FROM full_side l LEFT ANTI JOIN empty_side r ON l.x < r.x AND l.y > r.y ORDER BY l.id;

SELECT 'left of empty left', count() FROM empty_side l LEFT JOIN full_side r ON l.x < r.x AND l.y > r.y;
SELECT 'right of empty left';
SELECT * FROM empty_side l RIGHT JOIN full_side r ON l.x < r.x AND l.y > r.y ORDER BY r.id;
SELECT 'full of empty left';
SELECT * FROM empty_side l FULL JOIN full_side r ON l.x < r.x AND l.y > r.y ORDER BY r.id;
SELECT 'right semi of empty left', count() FROM empty_side l RIGHT SEMI JOIN full_side r ON l.x < r.x AND l.y > r.y;
SELECT 'right anti of empty left';
SELECT * FROM empty_side l RIGHT ANTI JOIN full_side r ON l.x < r.x AND l.y > r.y ORDER BY r.id;

SELECT 'both empty', count() FROM empty_side l FULL JOIN empty_side r ON l.x < r.x AND l.y > r.y;

SELECT 'join_use_nulls';
SET join_use_nulls = 1;
SELECT * FROM full_side l LEFT JOIN empty_side r ON l.x < r.x AND l.y > r.y ORDER BY l.id;
SELECT * FROM empty_side l FULL JOIN full_side r ON l.x < r.x AND l.y > r.y ORDER BY r.id;

DROP TABLE full_side;
DROP TABLE empty_side;
