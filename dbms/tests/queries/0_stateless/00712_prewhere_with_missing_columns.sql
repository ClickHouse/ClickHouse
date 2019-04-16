DROP TABLE IF EXISTS mergetree;
CREATE TABLE mergetree (x UInt8, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO mergetree VALUES (1, 'Hello, world!');
SELECT * FROM mergetree;

ALTER TABLE mergetree ADD COLUMN y UInt8 DEFAULT 0;
INSERT INTO mergetree VALUES (2, 'Goodbye.', 3);
SELECT * FROM mergetree ORDER BY x;

SELECT s FROM mergetree PREWHERE x AND y ORDER BY s;
SELECT s, y FROM mergetree PREWHERE x AND y ORDER BY s;

DROP TABLE mergetree;
