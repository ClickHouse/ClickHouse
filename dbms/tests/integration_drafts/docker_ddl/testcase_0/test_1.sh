#!/bin/bash
set -e

[[ `id -u -n` -ne root ]] && su
dce='docker-compose exec -T'

$dce ch1 clickhouse-client -q "CREATE TABLE IF NOT EXISTS all_tables ON CLUSTER 'cluster_no_replicas'
    (database String, name String, engine String, metadata_modification_time DateTime)
    ENGINE = Distributed('cluster_no_replicas', 'system', 'tables')" | cut -f 1-2 | sort

echo "###"

# Test default_database
$dce ch1 clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test2 ON CLUSTER 'cluster'" | cut -f 1-2 | sort
$dce ch1 clickhouse-client -q "CREATE TABLE null ON CLUSTER 'cluster2' (i Int8) ENGINE = Null" | cut -f 1-2 | sort
$dce ch1 clickhouse-client -q "SELECT hostName() AS h, database FROM all_tables WHERE name = 'null' ORDER BY h"
$dce ch1 clickhouse-client -q "DROP TABLE IF EXISTS null ON CLUSTER cluster2" | cut -f 1-2 | sort

echo "###"

# Replicated alter
$dce ch1 clickhouse-client -q "DROP TABLE IF EXISTS merge ON CLUSTER cluster" 1>/dev/null
$dce ch1 clickhouse-client -q "CREATE TABLE IF NOT EXISTS merge ON CLUSTER cluster (p Date, i Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', p, p, 1)" | cut -f 1-3 | sort
$dce ch1 clickhouse-client -q "CREATE TABLE IF NOT EXISTS all_merge_1 ON CLUSTER cluster (p Date, i Int32)
    ENGINE = Distributed(cluster, default, merge, i)" | cut -f 1-2 | uniq | wc -l
$dce ch1 clickhouse-client -q "CREATE TABLE IF NOT EXISTS all_merge_2 ON CLUSTER cluster (p Date, i Int64, s String)
    ENGINE = Distributed(cluster, default, merge, i)" | cut -f 1-2 | uniq | wc -l

$dce ch1 clickhouse-client -q "INSERT INTO all_merge_1 (i) VALUES (1) (2) (3) (4)"
$dce ch1 clickhouse-client -q "SELECT i FROM all_merge_1 ORDER BY i"
$dce ch1 clickhouse-client -q "ALTER TABLE merge ON CLUSTER cluster MODIFY COLUMN i Int64" | cut -f 1-2 | sort
$dce ch1 clickhouse-client -q "ALTER TABLE merge ON CLUSTER cluster ADD COLUMN s DEFAULT toString(i)" | cut -f 1-2 | sort
$dce ch1 clickhouse-client -q "SELECT i, s FROM all_merge_2 ORDER BY i"

echo "###"

# detach partition
$dce ch1 clickhouse-client -q "INSERT INTO all_merge_2 (p, i) VALUES (31, 5) (31, 6) (31, 7) (31, 8)"
$dce ch1 clickhouse-client -q "SELECT i FROM all_merge_2 ORDER BY i"
$dce ch1 clickhouse-client -q "ALTER TABLE merge ON CLUSTER cluster DETACH PARTITION 197002" | cut -f 1-2 | sort
$dce ch1 clickhouse-client -q "SELECT i FROM all_merge_2 ORDER BY i"

$dce ch1 clickhouse-client -q "DROP TABLE all_merge_1 ON CLUSTER cluster" | cut -f 1-3 | sort
$dce ch1 clickhouse-client -q "DROP TABLE all_merge_2 ON CLUSTER cluster" | cut -f 1-3 | sort
$dce ch1 clickhouse-client -q "DROP TABLE merge ON CLUSTER cluster" | cut -f 1-3 | sort

echo "###"

# Server fail
docker-compose kill ch2 1>/dev/null 2>/dev/null
($dce ch1 clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test2 ON CLUSTER 'cluster'" | cut -f 1-2 | sort) &
sleep 1
docker-compose start ch2 1>/dev/null 2>/dev/null
wait

echo "###"

# Connection loss
docker-compose pause ch2 zoo1 1>/dev/null 2>/dev/null
($dce ch1 clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test2 ON CLUSTER 'cluster'" | cut  -f 1-2 | sort) &
sleep 10
docker-compose unpause ch2 zoo1 1>/dev/null 2>/dev/null
wait

echo "###"

# Session expired
docker-compose pause ch2 zoo1 1>/dev/null 2>/dev/null
($dce ch1 clickhouse-client -q "CREATE DATABASE IF NOT EXISTS test2 ON CLUSTER 'cluster'" | cut  -f 1-2 | sort) &
sleep 31
docker-compose unpause ch2 zoo1 1>/dev/null 2>/dev/null
wait
