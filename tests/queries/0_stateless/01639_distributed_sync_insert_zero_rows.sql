-- Tags: distributed

DROP TABLE IF EXISTS local;
DROP TABLE IF EXISTS distributed;

CREATE TABLE local (x UInt8) ENGINE = Memory;
CREATE TABLE distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);

SET distributed_foreground_insert = 1;

INSERT INTO distributed SELECT number FROM numbers(256) WHERE number % 2 = 0;
SELECT count() FROM local;
SELECT count() FROM distributed;

TRUNCATE TABLE local;
INSERT INTO distributed SELECT number FROM numbers(256) WHERE number % 2 = 1;
SELECT count() FROM local;
SELECT count() FROM distributed;

TRUNCATE TABLE local;
INSERT INTO distributed SELECT number FROM numbers(256) WHERE number < 128;
SELECT count() FROM local;
SELECT count() FROM distributed;

DROP TABLE local;
DROP TABLE distributed;
