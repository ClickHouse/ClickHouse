DROP TABLE IF EXISTS local;
DROP TABLE IF EXISTS distributed;

CREATE TABLE local (x UInt8) Engine=Memory;
CREATE TABLE distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);

SET distributed_foreground_insert = 0, network_compression_method = 'zstd';

INSERT INTO distributed SELECT number FROM numbers(256);
SYSTEM FLUSH DISTRIBUTED distributed;

SELECT * from system.events WHERE event = 'InitialQuiery'
SELECT * from system.query_log WHERE type = 'InitialQuery'

