-- Tags: distributed

DROP TABLE IF EXISTS local;
DROP TABLE IF EXISTS distributed;

CREATE TABLE local (x UInt8) ENGINE = Memory;
CREATE TABLE distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);

SET insert_distributed_sync = 0, network_compression_method = 'zstd';

INSERT INTO distributed SELECT number FROM numbers(256);
SYSTEM FLUSH DISTRIBUTED distributed;

SELECT count() FROM local;
SELECT count() FROM distributed;

DROP TABLE local;
DROP TABLE distributed;
