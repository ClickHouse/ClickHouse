DROP TABLE IF EXISTS test.prewhere_defaults;

CREATE TABLE test.prewhere_defaults (d Date DEFAULT '2000-01-01', k UInt64 DEFAULT 0, x UInt16) ENGINE = MergeTree(d, k, 1);

INSERT INTO test.prewhere_defaults (x) VALUES (1);

SET max_block_size = 1;

SELECT * FROM test.prewhere_defaults PREWHERE x != 0 ORDER BY x;

ALTER TABLE test.prewhere_defaults ADD COLUMN y UInt16 DEFAULT x;

SELECT * FROM test.prewhere_defaults PREWHERE x != 0 ORDER BY x;

INSERT INTO test.prewhere_defaults (x) VALUES (2);

SELECT * FROM test.prewhere_defaults PREWHERE x != 0 ORDER BY x;

DROP TABLE test.prewhere_defaults;
