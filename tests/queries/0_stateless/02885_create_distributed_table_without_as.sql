-- Here a Distributed table without AS must detect its structure.

DROP TABLE IF EXISTS dist_tbl;
DROP TABLE IF EXISTS local_tbl;

CREATE TABLE local_tbl (`key` UInt32, `value` UInt32 DEFAULT 42) ENGINE = MergeTree ORDER BY key;
CREATE TABLE dist_tbl ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_tbl', rand());
SHOW CREATE TABLE dist_tbl;
INSERT INTO dist_tbl (key) SETTINGS distributed_foreground_insert=1 VALUES (99);
SELECT 'local_tbl';
SELECT * FROM local_tbl;
SELECT 'dist_tbl';
SELECT * FROM dist_tbl;

DROP TABLE dist_tbl;
DROP TABLE local_tbl;
