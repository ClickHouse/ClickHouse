 -- Tags: zookeeper

CREATE TABLE rmt (a UInt64, b UInt64)
ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', '1')
ORDER BY a;

INSERT INTO rmt VALUES (1, 4);

SYSTEM STOP MERGES rmt;
ALTER TABLE rmt UPDATE b = 10 WHERE a != 0;
ALTER TABLE rmt RENAME COLUMN b to c; -- {serverError BAD_ARGUMENTS};

SYSTEM START MERGES rmt;
DROP TABLE rmt SYNC;
