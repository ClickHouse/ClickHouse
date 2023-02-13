CREATE TABLE test_02537 (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE test_dist_02537 (n Int8) ENGINE=Distributed(test_cluster_two_shards, currentDatabase(), test_02537) SETTINGS bytes_to_delay_insert=1;

INSERT INTO test_dist_02537 SELECT number FROM numbers(5);
SELECT sum(n), count(n) FROM test_dist_02537;
SYSTEM STOP DISTRIBUTED SENDS test_dist_02537;

-- MEMORY_EXCEEDED
insert into test_dist_02537 select number from system.numbers limit 10000000 settings max_memory_usage='10Mi', max_untracked_memory=0, prefer_localhost_replica=0, distributed_directory_monitor_batch_inserts=1, distributed_directory_monitor_split_batch_on_failure=1;


DETACH TABLE test_dist_02537;

ATTACH TABLE test_dist_02537;
