DROP TABLE IF EXISTS prewhere_defaults;

CREATE TABLE prewhere_defaults (d Date DEFAULT '2000-01-01', k UInt64 DEFAULT 0, x UInt16) ENGINE = MergeTree(d, k, 1);

INSERT INTO prewhere_defaults (x) VALUES (1);

SET max_block_size = 1;

SELECT * FROM prewhere_defaults PREWHERE x != 0 ORDER BY x;

ALTER TABLE prewhere_defaults ADD COLUMN y UInt16 DEFAULT x;

SELECT * FROM prewhere_defaults PREWHERE x != 0 ORDER BY x;

INSERT INTO prewhere_defaults (x) VALUES (2);

SELECT * FROM prewhere_defaults PREWHERE x != 0 ORDER BY x;

DROP TABLE prewhere_defaults;
