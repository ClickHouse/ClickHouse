-- Tags: distributed, replica, shard, zookeeper

DROP TABLE IF EXISTS shared_test_table;
DROP TABLE IF EXISTS distributed_test_table;

CREATE TABLE shared_test_table 
ON CLUSTER test_cluster_two_shard_three_replicas_localhost (id Int16) 
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_02985_{database}/{shard}/shared_test_table', '{replica}') 
ORDER BY (id);

CREATE TABLE distributed_test_table 
ON CLUSTER test_cluster_two_shard_three_replicas_localhost (id Int16) 
ENGINE = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), shared_test_table);

INSERT INTO shared_test_table VALUES (123), (651), (446), (315), (234), (764);

SELECT id FROM distributed_test_table LIMIT 3;

SET dialect = 'kusto';

distributed_test_table | take 3;

SET dialect = 'prql';

from distributed_test_table
select {id}
take 1..3
