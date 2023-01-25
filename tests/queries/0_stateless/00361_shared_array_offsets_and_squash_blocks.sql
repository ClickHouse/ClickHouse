DROP TABLE IF EXISTS nested1;
DROP TABLE IF EXISTS nested2;

CREATE TABLE nested1 (d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, b String)) ENGINE = MergeTree(d, x, 1);
INSERT INTO nested1 (x, n.a, n.b) VALUES (1, ['Hello', 'World'], ['abc', 'def']), (2, [], []);

SET max_block_size = 1;
SELECT * FROM nested1 ORDER BY x;

CREATE TABLE nested2 (d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, b String)) ENGINE = MergeTree(d, x, 1);

INSERT INTO nested2 SELECT * FROM nested1;

SELECT * FROM nested2 ORDER BY x;

DROP TABLE nested1;
DROP TABLE nested2;
