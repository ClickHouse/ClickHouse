-- Tags: no-old-analyzer

-- Edge cases of the IEJoin residual condition: a residual that yields NULL (counts as failed),
-- empty inputs, a String residual column alongside encodable numeric keys, and max_block_size=1
-- (the residual is evaluated per single-pair batch).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS tel;
DROP TABLE IF EXISTS ter;

CREATE TABLE tel (id UInt32, x Int32, y Int32, s String, n Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ter (id UInt32, x Int32, y Int32, s String, n Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tel VALUES (1, 0, 10, 'aa', 1), (2, 1, 9, 'bb', NULL), (3, 2, 8, 'cc', 3);
INSERT INTO ter VALUES (1, 5, 5, 'a%', 2), (2, 6, 4, 'b%', NULL), (3, 7, 3, 'x%', 3);

-- A NULL residual is a failed match: rows 2 (NULL n) never match, row 3 matches only n=3
SELECT 'null residual left';
SELECT l.id, r.id FROM tel l LEFT JOIN ter r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL;
SELECT 'null residual semi';
SELECT l.id FROM tel l LEFT SEMI JOIN ter r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL;
SELECT 'null residual anti';
SELECT l.id FROM tel l LEFT ANTI JOIN ter r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL;

-- String residual over encoded numeric inequality keys
SELECT 'string residual';
SELECT l.id, r.id FROM tel l LEFT JOIN ter r ON l.x < r.x AND l.y > r.y AND l.s LIKE r.s ORDER BY ALL;

-- Every pair is emitted through a single-pair batch
SELECT 'max_block_size 1';
SELECT l.id, r.id FROM tel l FULL JOIN ter r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL SETTINGS max_block_size = 1;

-- Empty side(s) with a residual: unmatched rows of the other side are still emitted
SELECT 'empty right left-join';
SELECT l.id, r.id FROM tel l LEFT JOIN (SELECT * FROM ter WHERE 0) r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL;
SELECT 'empty left right-join';
SELECT l.id, r.id FROM (SELECT * FROM tel WHERE 0) l RIGHT JOIN ter r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL;
SELECT 'empty both full-join';
SELECT count() FROM (SELECT * FROM tel WHERE 0) l FULL JOIN (SELECT * FROM ter WHERE 0) r ON l.x < r.x AND l.y > r.y AND l.n <= r.n;
SELECT 'empty right anti';
SELECT l.id FROM tel l LEFT ANTI JOIN (SELECT * FROM ter WHERE 0) r ON l.x < r.x AND l.y > r.y AND l.n <= r.n ORDER BY ALL;

DROP TABLE tel;
DROP TABLE ter;
