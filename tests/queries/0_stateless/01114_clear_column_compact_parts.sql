DROP TABLE IF EXISTS clear_column;

CREATE TABLE clear_column(x UInt32, y UInt32) ENGINE MergeTree ORDER BY x PARTITION by x;
INSERT INTO clear_column VALUES (1, 1), (2, 3);

ALTER TABLE clear_column CLEAR COLUMN y IN PARTITION 1;
SELECT * FROM clear_column ORDER BY x;
ALTER TABLE clear_column CLEAR COLUMN y IN PARTITION 2;
SELECT * FROM clear_column ORDER BY x;

DROP TABLE clear_column;
