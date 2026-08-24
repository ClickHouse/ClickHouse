DROP TABLE IF EXISTS mergetree_00712;
CREATE TABLE mergetree_00712 (x UInt8, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO mergetree_00712 VALUES (1, 'Hello, world!');
SELECT * FROM mergetree_00712;

ALTER TABLE mergetree_00712 ADD COLUMN y UInt8 DEFAULT 0;
INSERT INTO mergetree_00712 VALUES (2, 'Goodbye.', 3);
SELECT * FROM mergetree_00712 ORDER BY x;

SELECT s FROM mergetree_00712 PREWHERE x AND y ORDER BY s;
SELECT s, y FROM mergetree_00712 PREWHERE x AND y ORDER BY s;

DROP TABLE mergetree_00712;
